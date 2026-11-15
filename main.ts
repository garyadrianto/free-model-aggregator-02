// free-model-aggregator-02 — one server, two client wires, a live free model behind every answer.
//
// WHAT A TRIAL GIVES YOU
//   A trial runs this server as `node /bot/main.ts` from the repository root, with Node 22's standard
//   library alone: nothing is installed, TypeScript runs directly, and a relative import would need its
//   .ts extension (this entry has no relative imports — it is one self-contained file on purpose).
//
//   The env is at $MAKEFUN_ENV_URL. Model calls go through the grader's gateway at $OPENAI_BASE_URL, an
//   OpenAI-compatible endpoint that reaches OpenRouter on the key pushed to the competition. The brief at
//   $MAKEFUN_PROMPT_FILE carries two JSON documents:
//     - the free model list: each free model's gateway id (`openrouter/<vendor>/<model>:free`), its
//       published rating in milli-points, what it can do (tools, vision, JSON mode, context length);
//     - the six prompts: each one's id, its api (openai or anthropic), whether it streams, its weight
//       (1 or 2) and the requirements a model must meet. Each prompt carries a unique token the answer
//       must contain.
//
// THE FOUR THINGS THAT DECIDE THE SCORE (quality 60 / served 15 / wire 10 / failover 10 / catalog 5)
//   1. ROUTING BY RATING. Quality pays rating(answered)/best_rating(requirements), so every prompt gets
//      a ladder of the models that actually meet its requirements, ordered by the brief's rating_milli
//      descending, with the id as the tie-break so the order is a pure function of the brief.
//   2. FAILOVER WITH A TRAIL. A refusal is retried on the next model in the ladder, and each refusal is
//      recorded into the `x-makefun-upstream-failures` header — the only channel the house can read a
//      retried refusal from. At most 8 rows, under 8 KiB, and no row longer than the cap allows.
//   3. A NORMALISED STREAM. The gateway's SSE framing is never relayed: the whole upstream answer is
//      read, then one canonical sequence is written (role chunk, content deltas, exactly one finish
//      chunk, one usage chunk, [DONE]). Two finish chunks is a scored failure, so only we write them.
//   4. A COOLING SET. A model that refused is not dialled again this run, so one 429 cannot take every
//      later prompt down with it. The set is ordered by first refusal, which keeps replays identical.
//
// DETERMINISM. Nothing here reads the clock, a timer or a random source, so a replay of the same
// cassette writes the same body, the same stream and the same state. Chat ids and created timestamps are
// constants for that reason, not laziness.
//
// THE MODEL RULE. Whatever this server reports in a response's `model` field must be the model it
// actually dialled. The gateway strips its `openrouter/` prefix in the provider's answer, so the dialled
// id is put back on the JSON answer, on every streamed chunk and inside message_start.message.model.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { readFileSync } from 'node:fs';

// ─────────────────────────────────────────────────────────────── constants ──

const PROTOCOL_VERSION = 1;
const POLL_INTERVAL_MS = 100;
const MAX_BODY_BYTES = 8 * 1024 * 1024;

/** The brief's schemas carry this prefix; a document is found by it, never by a key name alone. */
const SCHEMA_PREFIX = 'free-model-aggregator-02/';
const SCORED_MODELS_SCHEMA = 'free-model-aggregator-02/models/v1';
const SCORED_PROMPTS_SCHEMA = 'free-model-aggregator-02/prompts/v1';

/** The provider's public endpoint, never the gateway's trial-local address (the replay compares it). */
const OPENROUTER_PUBLIC_BASE = 'https://openrouter.ai/api/v1';
const OPENROUTER_KEY_ENV = 'OPENROUTER_API_KEY';

/** OpenAI list replies carry a `created`; a moving one would make the document nondeterministic. */
const FIXED_CREATED = 1767225600;
const FIXED_CREATED_AT = '2026-01-01T00:00:00Z';
const FIXED_RESPONSE_ID = 'chatcmpl-aggregator';

/** Caps the house reads on the failure trail: more rows, or more bytes, is read as no trail at all. */
const MAX_FAILURE_ROWS = 8;
const MAX_FAILURE_BYTES = 8 * 1024;
const MAX_FAILURE_CODE = 64;
const MAX_FAILURE_MODEL = 200;

/** One gateway call's budget. Six prompts share a 240 s wall, so a slow model is abandoned, not waited on. */
const DIAL_TIMEOUT_MS = 45_000;
/**
 * The gateway is dialled at most this many times for one prompt, refusals included. The cap costs
 * nothing on the clean path — a prompt that is served first time is one call — and it only spends more
 * when earlier models refuse, where paying an extra call beats losing the prompt outright.
 */
const MAX_ATTEMPTS = 6;

/** The output cap defaulted onto a client that omitted one; `max_tokens_required` is a refused prompt. */
const DEFAULT_MAX_TOKENS = 1024;

// ────────────────────────────────────────────────────────────── json shapes ──

type Json = Record<string, unknown>;

const isRecord = (v: unknown): v is Json => v !== null && typeof v === 'object' && !Array.isArray(v);

const asString = (v: unknown): string => (typeof v === 'string' ? v : '');
const asNumberOrNull = (v: unknown): number | null =>
  typeof v === 'number' && Number.isFinite(v) ? v : null;
const asBoolOrNull = (v: unknown): boolean | null => (typeof v === 'boolean' ? v : null);
const asCount = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) ? v : 0);

interface ModelRow {
  id: string;
  provider: string;
  display_name: string;
  rating_milli: number;
  context_length: number | null;
  tools: boolean | null;
  vision: boolean | null;
  json: boolean | null;
  free: boolean;
}

interface Requirement {
  tools?: boolean;
  vision?: boolean;
  json?: boolean;
  min_context?: number;
}

interface PromptRow {
  id: string;
  api: 'openai' | 'anthropic';
  stream: boolean;
  weight: 1 | 2;
  requires: Requirement;
}

interface Brief {
  models: ModelRow[];
  prompts: PromptRow[];
  error: string | null;
}

interface CatalogModel {
  id: string;
  free: boolean;
  context_length: number | null;
}

interface CatalogReport {
  providers: Array<{
    id: string;
    base_url: string;
    key_env: string;
    wire: 'openai';
    models: CatalogModel[];
  }>;
}

/** One row of the failure trail: a refusal the house could otherwise never see. */
interface Failure {
  status: number;
  code: string;
  model: string | null;
}

// ─────────────────────────────────────────────────────────── brief parsing ──

/**
 * The index of the `}` that closes the object opening at `start`, or -1. String contents are skipped so
 * a brace inside a prompt's prose cannot end the document early.
 */
function objectEnd(text: string, start: number): number {
  let depth = 0;
  let inString = false;
  for (let i = start; i < text.length; i += 1) {
    const ch = text[i];
    if (inString) {
      if (ch === '\\') i += 1;
      else if (ch === '"') inString = false;
    } else if (ch === '"') {
      inString = true;
    } else if (ch === '{') {
      depth += 1;
    } else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return -1;
}

/** Every JSON object in the brief that carries a string `schema`, in the order they appear. */
function documentsIn(text: string): Json[] {
  const found: Json[] = [];
  for (let start = text.indexOf('{'); start !== -1; start = text.indexOf('{', start + 1)) {
    const end = objectEnd(text, start);
    if (end === -1) continue;
    try {
      const parsed: unknown = JSON.parse(text.slice(start, end + 1));
      if (isRecord(parsed) && typeof parsed.schema === 'string') found.push(parsed);
    } catch {
      // Not a standalone JSON object: keep scanning.
    }
  }
  return found;
}

/** The first document under this contest's prefix holding one of `keys` as an array. */
function arrayDocument(docs: readonly Json[], keys: readonly string[]): { doc: Json; key: string } | null {
  for (const doc of docs) {
    if (!asString(doc.schema).includes(SCHEMA_PREFIX)) continue;
    for (const key of keys) if (Array.isArray(doc[key])) return { doc, key };
  }
  return null;
}

/**
 * One model row. A row naming `openrouter` with a bare id is routed as the gateway spells it, since that
 * prefix is what the gateway routes on; a row that already carries it is left alone.
 */
function modelOf(raw: unknown): ModelRow | null {
  if (!isRecord(raw)) return null;
  const idOrModel = asString(raw.id) !== '' ? asString(raw.id) : asString(raw.model);
  if (idOrModel === '') return null;
  const named = asString(raw.provider) !== '' ? asString(raw.provider) : asString(raw.wire);
  const id =
    named === 'openrouter' && !idOrModel.toLowerCase().startsWith('openrouter/')
      ? `openrouter/${idOrModel}`
      : idOrModel;
  return {
    id,
    provider: named === '' ? 'openrouter' : named,
    display_name: asString(raw.display_name) !== '' ? asString(raw.display_name) : idOrModel,
    rating_milli: asNumberOrNull(raw.rating_milli) ?? 0,
    context_length: asNumberOrNull(raw.context_length),
    tools: asBoolOrNull(raw.tools),
    vision: asBoolOrNull(raw.vision),
    json: asBoolOrNull(raw.json),
    free: raw.free !== false,
  };
}

/** One prompt row: only requirements set to `true` are constraints, so `false` is not a filter. */
function promptOf(raw: unknown): PromptRow | null {
  if (!isRecord(raw)) return null;
  const id = asString(raw.id);
  if (id === '') return null;
  const wants = isRecord(raw.requires) ? raw.requires : {};
  const minContext = asNumberOrNull(wants.min_context);
  return {
    id,
    api: raw.api === 'anthropic' ? 'anthropic' : 'openai',
    stream: raw.stream === true,
    weight: raw.weight === 2 ? 2 : 1,
    requires: {
      ...(wants.tools === true ? { tools: true } : {}),
      ...(wants.vision === true ? { vision: true } : {}),
      ...(wants.json === true ? { json: true } : {}),
      ...(minContext !== null ? { min_context: minContext } : {}),
    },
  };
}

/**
 * The brief at `path`. The model list may be spelled `models` or `ratings` and the prompt list `prompts`
 * or `manifest`; a document is matched by the arrays it carries, so either spelling reads.
 */
function readBrief(path: string): Brief {
  if (path === '') return { models: [], prompts: [], error: 'MAKEFUN_PROMPT_FILE is not set' };
  let text: string;
  try {
    text = readFileSync(path, 'utf8');
  } catch (err) {
    return { models: [], prompts: [], error: `cannot read ${path}: ${(err as Error).message}` };
  }
  const docs = documentsIn(text);

  const modelsDoc = arrayDocument(docs, ['models', 'ratings']);
  if (modelsDoc === null) {
    return { models: [], prompts: [], error: `${path} holds no ${SCORED_MODELS_SCHEMA} document` };
  }
  const models = (modelsDoc.doc[modelsDoc.key] as unknown[]).flatMap((raw) => {
    const row = modelOf(raw);
    return row === null ? [] : [row];
  });

  const promptsDoc = arrayDocument(docs, ['prompts', 'manifest']);
  if (promptsDoc === null) {
    return { models, prompts: [], error: `${path} holds no ${SCORED_PROMPTS_SCHEMA} document` };
  }
  const prompts = (promptsDoc.doc[promptsDoc.key] as unknown[]).flatMap((raw) => {
    const row = promptOf(raw);
    return row === null ? [] : [row];
  });

  return { models, prompts, error: null };
}

const brief = readBrief(process.env.MAKEFUN_PROMPT_FILE ?? '');

/**
 * The catalog: one provider, exactly the free model ids the brief names, deduplicated in brief order.
 * The catalog column compares the registered set and both GET /v1/models answers against exactly these
 * ids, so listing fewer loses it as surely as listing more.
 */
function buildCatalog(): CatalogReport {
  const seen = new Set<string>();
  const models: CatalogModel[] = [];
  for (const model of brief.models) {
    if (!model.free || seen.has(model.id)) continue;
    seen.add(model.id);
    models.push({ id: model.id, free: true, context_length: model.context_length });
  }
  if (models.length === 0) return { providers: [] };
  return {
    providers: [
      {
        id: 'openrouter',
        base_url: OPENROUTER_PUBLIC_BASE,
        key_env: OPENROUTER_KEY_ENV,
        wire: 'openai',
        models,
      },
    ],
  };
}

const catalog: CatalogReport = buildCatalog();

// ──────────────────────────────────────────────────────────────── routing ──

/** Whether a model can serve a prompt. An unknown capability never satisfies a requirement. */
function meets(requires: Requirement, model: ModelRow): boolean {
  if (requires.tools === true && model.tools !== true) return false;
  if (requires.vision === true && model.vision !== true) return false;
  if (requires.json === true && model.json !== true) return false;
  if (requires.min_context !== undefined) {
    if (model.context_length === null || model.context_length < requires.min_context) return false;
  }
  return true;
}

/**
 * The ladder for one prompt: every free model that meets it, best rating first, id ascending on a tie.
 * A geo-blocked top model stays in first place — a 403 is the grading host's region, not a reason to
 * drop the model, so the ladder falls through it instead.
 */
function ladderFor(requires: Requirement): ModelRow[] {
  const eligible = brief.models.filter((model) => model.free && meets(requires, model));
  return eligible.sort((a, b) => {
    if (b.rating_milli !== a.rating_milli) return b.rating_milli - a.rating_milli;
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
  });
}

/** Models that already refused this run, in first-refusal order. Deterministic, so replays match. */
const cooling = new Set<string>();

/**
 * The order to try for one prompt: models not yet cooling first (best rating first), then the cooling
 * ones as the last resort, because a prompt served by a cooling model still beats an unserved prompt.
 */
function planFor(requires: Requirement): ModelRow[] {
  const ladder = ladderFor(requires);
  const fresh = ladder.filter((model) => !cooling.has(model.id));
  const cold = ladder.filter((model) => cooling.has(model.id));
  return [...fresh, ...cold].slice(0, MAX_ATTEMPTS);
}

/** Which brief prompt the request now arriving is: the brief lists them in the client's arrival order. */
let manifestIndex = 0;

/** Requirements of the arriving prompt: the brief's row by arrival order, else read off the body. */
function requirementsFor(body: Json): Requirement {
  const row = brief.prompts[manifestIndex];
  manifestIndex += 1;
  if (row !== undefined) return row.requires;
  return requirementsFromBody(body);
}

/** Requirements inferred from a body, for a brief that holds no row for it. */
function requirementsFromBody(body: Json): Requirement {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  let vision = false;
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) {
      if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
    }
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return {
    tools: Array.isArray(body.tools) && body.tools.length > 0,
    vision,
    json: format === 'json_object' || format === 'json_schema',
  };
}

// ────────────────────────────────────────────────────────────────── dial ──

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const GATEWAY_URL = (process.env.OPENAI_BASE_URL ?? '').replace(/\/+$/, '');
/** The trial's dummy gateway key; the gateway swaps it for the key the entrant pushed. */
const GATEWAY_KEY = process.env.OPENAI_API_KEY ?? '';

function log(message: string): void {
  process.stderr.write(`aggregator: ${message}\n`);
}

/** The gateway's Chat Completions route; a base without `/v1` gets one so both spellings reach it. */
function chatUrl(): string {
  if (GATEWAY_URL === '') return '';
  return GATEWAY_URL.endsWith('/v1') ? `${GATEWAY_URL}/chat/completions` : `${GATEWAY_URL}/v1/chat/completions`;
}

/**
 * One gateway call, naming the model this entry dialled. Streaming is always requested so the usage
 * chunk arrives; the relay below buffers it, so a stream request does not mean a streamed answer.
 */
async function dial(modelId: string, body: Json): Promise<Response> {
  const wantsStream = body.stream === true;
  const payload: Json = { ...body, model: modelId };
  let headers: Record<string, string> = {
    'content-type': 'application/json',
    authorization: `Bearer ${GATEWAY_KEY}`,
  };
  if (wantsStream) {
    payload.stream_options = { include_usage: true };
    headers = { ...headers, accept: 'text/event-stream' };
  }
  return fetch(chatUrl(), {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(DIAL_TIMEOUT_MS),
  });
}

/** A refusal's short machine code: the upstream's `error.code`, else its `error.type`, else 'refused'. */
function refusalCode(status: number, text: string): string {
  try {
    const parsed: unknown = JSON.parse(text);
    if (isRecord(parsed) && isRecord(parsed.error)) {
      const code = asString(parsed.error.code);
      if (code !== '') return code.slice(0, MAX_FAILURE_CODE);
      const type = asString(parsed.error.type);
      if (type !== '') return type.slice(0, MAX_FAILURE_CODE);
      const message = asString(parsed.error.message);
      if (message !== '') return message.slice(0, MAX_FAILURE_CODE);
    }
  } catch {
    // Not JSON: fall through to the status-named code.
  }
  return `http_${status}`;
}

/**
 * The failure trail header. Rows are dropped from the end when the document would pass the byte cap, so
 * what is written is always a well-formed list the house can read: a truncated one counts as no trail.
 */
function failureTrail(failures: readonly Failure[]): string {
  const rows = failures.slice(0, MAX_FAILURE_ROWS).map((failure) => ({
    status: Math.max(100, Math.min(599, Math.trunc(failure.status))),
    code: failure.code.slice(0, MAX_FAILURE_CODE),
    model: failure.model === null ? null : failure.model.slice(0, MAX_FAILURE_MODEL),
  }));
  while (rows.length > 0 && Buffer.byteLength(JSON.stringify(rows)) > MAX_FAILURE_BYTES) rows.pop();
  return rows.length === 0 ? '' : JSON.stringify(rows);
}

// ───────────────────────────────────────────── upstream answer → plain text ──

interface UpstreamAnswer {
  /** The answer's text: every content delta joined, or the JSON message's content. */
  text: string;
  /** The finish reason the provider named, normalised to OpenAI's vocabulary. */
  finish: string;
  inputTokens: number;
  outputTokens: number;
  /** Tool calls the provider streamed, already whole: id, name and the arguments JSON. */
  toolCalls: Array<{ id: string; name: string; arguments: string }>;
}

/** A finish reason in the vocabulary the wire check accepts; anything unrecognised reads as `stop`. */
function normalFinish(raw: unknown): string {
  if (raw === 'length') return 'length';
  if (raw === 'tool_calls' || raw === 'function_call') return 'tool_calls';
  if (raw === 'content_filter') return 'content_filter';
  return 'stop';
}

/** The text of a message's content: a plain string, or a block array's text blocks. */
function contentText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content
    .flatMap((block) => (isRecord(block) && block.type === 'text' && typeof block.text === 'string' ? [block.text] : []))
    .join('');
}

/** The completion tokens to report. The wire check needs output >= 1, so an absent count is derived. */
function deriveOutputTokens(reported: number, text: string): number {
  if (reported >= 1) return reported;
  return Math.max(1, Math.ceil(text.length / 4));
}

/** Tool calls as the provider streamed them, rebuilt by index into whole calls. */
function collectToolCalls(partial: Map<number, { id: string; name: string; arguments: string }>): Array<{ id: string; name: string; arguments: string }> {
  return [...partial.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([, call]) => call)
    .filter((call) => call.name !== '');
}

/**
 * Read a gateway SSE stream to its end and reduce it to one answer. Buffering the whole stream is what
 * makes the relay deterministic and lets the finish chunk carry the usage the provider sends last.
 */
async function readStream(body: ReadableStream<Uint8Array>): Promise<UpstreamAnswer> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let text = '';
  let finish = 'stop';
  let inputTokens = 0;
  let outputTokens = 0;
  const toolCalls = new Map<number, { id: string; name: string; arguments: string }>();

  const takeFrame = (frame: string): void => {
    for (const line of frame.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed.startsWith('data:')) continue;
      const payload = trimmed.slice(5).trim();
      if (payload === '' || payload === '[DONE]') continue;
      let chunk: unknown;
      try {
        chunk = JSON.parse(payload);
      } catch {
        continue;
      }
      if (!isRecord(chunk)) continue;
      if (isRecord(chunk.usage)) {
        inputTokens = asCount(chunk.usage.prompt_tokens);
        outputTokens = asCount(chunk.usage.completion_tokens);
      }
      const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
      const choice = isRecord(choices[0]) ? choices[0] : null;
      if (choice === null) continue;
      const delta = isRecord(choice.delta) ? choice.delta : {};
      const piece = asString(delta.content);
      if (piece !== '') text += piece;
      const calls = Array.isArray(delta.tool_calls) ? delta.tool_calls : [];
      for (const raw of calls) {
        if (!isRecord(raw)) continue;
        const index = typeof raw.index === 'number' ? raw.index : toolCalls.size;
        const current = toolCalls.get(index) ?? { id: '', name: '', arguments: '' };
        const fn = isRecord(raw.function) ? raw.function : {};
        if (asString(raw.id) !== '') current.id = asString(raw.id);
        if (asString(fn.name) !== '') current.name = asString(fn.name);
        if (asString(fn.arguments) !== '') current.arguments += asString(fn.arguments);
        toolCalls.set(index, current);
      }
      if (choice.finish_reason !== null && choice.finish_reason !== undefined) finish = normalFinish(choice.finish_reason);
    }
  };

  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
      let at = buffer.indexOf('\n\n');
      while (at !== -1) {
        takeFrame(buffer.slice(0, at));
        buffer = buffer.slice(at + 2);
        at = buffer.indexOf('\n\n');
      }
    }
    buffer += decoder.decode();
    if (buffer.trim() !== '') takeFrame(buffer);
  } finally {
    reader.releaseLock();
  }

  const whole = collectToolCalls(toolCalls);
  return {
    text,
    finish: whole.length > 0 && finish === 'stop' ? 'tool_calls' : finish,
    inputTokens,
    outputTokens: deriveOutputTokens(outputTokens, text),
    toolCalls: whole,
  };
}

/** A non-streaming gateway answer reduced to the same shape, so both paths share one writer. */
function readJsonAnswer(body: Json): UpstreamAnswer {
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const choice = isRecord(choices[0]) ? choices[0] : {};
  const message = isRecord(choice.message) ? choice.message : {};
  const text = contentText(message.content);
  const usage = isRecord(body.usage) ? body.usage : {};
  const calls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
  const toolCalls = calls.flatMap((raw) => {
    if (!isRecord(raw) || !isRecord(raw.function)) return [];
    return [
      {
        id: asString(raw.id),
        name: asString(raw.function.name),
        arguments: typeof raw.function.arguments === 'string' ? raw.function.arguments : JSON.stringify(raw.function.arguments ?? {}),
      },
    ];
  });
  return {
    text,
    finish: toolCalls.length > 0 ? 'tool_calls' : normalFinish(choice.finish_reason),
    inputTokens: asCount(usage.prompt_tokens),
    outputTokens: deriveOutputTokens(asCount(usage.completion_tokens), text),
    toolCalls,
  };
}

// ─────────────────────────────────────────────────────── answer → two wires ──

/** One request's answer, in the shape both wire writers read. */
interface Served {
  answer: UpstreamAnswer;
  model: string;
  failures: Failure[];
}

/** An SSE frame: an optional `event:` line, then the JSON data line, then the blank line. */
function sse(event: string | null, data: unknown): string {
  return `${event === null ? '' : `event: ${event}\n`}data: ${JSON.stringify(data)}\n\n`;
}

/** A token count as a whole number: the wire check reads `input >= 0` and `output >= 1`. */
const whole = (n: number): number => (Number.isFinite(n) && n > 0 ? Math.trunc(n) : 0);

/**
 * The OpenAI wire: one JSON answer, `model` the id actually dialled and whole usage. The provider's own
 * spelling of the model is discarded here — the claim the house scores is the call this entry made.
 */
function openAIJson(served: Served): Json {
  const { answer, model } = served;
  const message: Json = { role: 'assistant', content: answer.text };
  if (answer.toolCalls.length > 0) {
    message.tool_calls = answer.toolCalls.map((call, index) => ({
      id: call.id === '' ? `call_${index}` : call.id,
      type: 'function',
      function: { name: call.name, arguments: call.arguments === '' ? '{}' : call.arguments },
    }));
    if (answer.text === '') message.content = null;
  }
  return {
    id: FIXED_RESPONSE_ID,
    object: 'chat.completion',
    created: FIXED_CREATED,
    model,
    choices: [{ index: 0, message, finish_reason: answer.finish }],
    usage: {
      prompt_tokens: whole(answer.inputTokens),
      completion_tokens: Math.max(1, whole(answer.outputTokens)),
      total_tokens: whole(answer.inputTokens) + Math.max(1, whole(answer.outputTokens)),
    },
  };
}

/**
 * The OpenAI stream, normalised. Exactly one finish chunk and exactly one usage chunk are written,
 * whatever framing the provider used: a repeated finish chunk or a missing usage chunk is a scored
 * failure on the wire column, and the only way to guarantee their count is to write them ourselves.
 */
function openAIStream(served: Served): string[] {
  const { answer, model } = served;
  const base = { id: FIXED_RESPONSE_ID, object: 'chat.completion.chunk', created: FIXED_CREATED, model };
  const frames: string[] = [];

  frames.push(sse(null, { ...base, choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }] }));

  if (answer.text !== '') {
    frames.push(sse(null, { ...base, choices: [{ index: 0, delta: { content: answer.text }, finish_reason: null }] }));
  }

  if (answer.toolCalls.length > 0) {
    const toolDeltas = answer.toolCalls.map((call, index) => ({
      index,
      id: call.id === '' ? `call_${index}` : call.id,
      type: 'function',
      function: { name: call.name, arguments: call.arguments === '' ? '{}' : call.arguments },
    }));
    frames.push(sse(null, { ...base, choices: [{ index: 0, delta: { tool_calls: toolDeltas }, finish_reason: null }] }));
  }

  frames.push(sse(null, { ...base, choices: [{ index: 0, delta: {}, finish_reason: answer.finish }] }));

  const completion = Math.max(1, whole(answer.outputTokens));
  frames.push(sse(null, { ...base, choices: [], usage: { prompt_tokens: whole(answer.inputTokens), completion_tokens: completion, total_tokens: whole(answer.inputTokens) + completion } }));
  frames.push('data: [DONE]\n\n');
  return frames;
}

/** A Chat Completions answer as an Anthropic Messages object, with the dialled model named back. */
function anthropicJson(served: Served): Json {
  const { answer, model } = served;
  const content: Json[] = [];
  if (answer.text !== '') content.push({ type: 'text', text: answer.text });
  for (const [index, call] of answer.toolCalls.entries()) {
    let input: Json = {};
    try {
      const parsed: unknown = JSON.parse(call.arguments === '' ? '{}' : call.arguments);
      if (isRecord(parsed)) input = parsed;
    } catch {
      input = {};
    }
    content.push({ type: 'tool_use', id: call.id === '' ? `toolu_${index}` : call.id, name: call.name, input });
  }
  if (content.length === 0) content.push({ type: 'text', text: '' });
  return {
    id: `msg_${FIXED_RESPONSE_ID}`,
    type: 'message',
    role: 'assistant',
    model,
    content,
    stop_reason: anthropicStop(answer.finish),
    stop_sequence: null,
    usage: { input_tokens: whole(answer.inputTokens), output_tokens: Math.max(1, whole(answer.outputTokens)) },
  };
}

/** OpenAI's finish reason as Anthropic's stop reason. */
function anthropicStop(finish: string): string {
  if (finish === 'length') return 'max_tokens';
  if (finish === 'tool_calls') return 'tool_use';
  if (finish === 'content_filter') return 'refusal';
  return 'end_turn';
}

/**
 * The Anthropic stream in the order its grammar requires: message_start first, then per block
 * start/delta/stop, then message_delta carrying the stop reason and the output tokens, then
 * message_stop, and nothing after it.
 */
function anthropicStream(served: Served): string[] {
  const { answer, model } = served;
  const outTokens = Math.max(1, whole(answer.outputTokens));
  const events: string[] = [];
  let index = 0;

  events.push(
    sse('message_start', {
      type: 'message_start',
      message: {
        id: `msg_${FIXED_RESPONSE_ID}`,
        type: 'message',
        role: 'assistant',
        model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: whole(answer.inputTokens), output_tokens: 0 },
      },
    }),
  );

  if (answer.text !== '') {
    const textIndex = index;
    index += 1;
    events.push(sse('content_block_start', { type: 'content_block_start', index: textIndex, content_block: { type: 'text', text: '' } }));
    events.push(sse('content_block_delta', { type: 'content_block_delta', index: textIndex, delta: { type: 'text_delta', text: answer.text } }));
    events.push(sse('content_block_stop', { type: 'content_block_stop', index: textIndex }));
  }

  for (const call of answer.toolCalls) {
    const toolIndex = index;
    index += 1;
    events.push(
      sse('content_block_start', {
        type: 'content_block_start',
        index: toolIndex,
        content_block: { type: 'tool_use', id: call.id === '' ? `toolu_${toolIndex}` : call.id, name: call.name, input: {} },
      }),
    );
    events.push(
      sse('content_block_delta', {
        type: 'content_block_delta',
        index: toolIndex,
        delta: { type: 'input_json_delta', partial_json: call.arguments === '' ? '{}' : call.arguments },
      }),
    );
    events.push(sse('content_block_stop', { type: 'content_block_stop', index: toolIndex }));
  }

  events.push(
    sse('message_delta', {
      type: 'message_delta',
      delta: { stop_reason: anthropicStop(answer.finish), stop_sequence: null },
      usage: { output_tokens: outTokens },
    }),
  );
  events.push(sse('message_stop', { type: 'message_stop' }));
  return events;
}

// ────────────────────────────────────────────────── upstream → Anthropic body ──

/** An Anthropic image block as an OpenAI `image_url` part, for a base64 or URL source. */
function imagePart(block: Json): Json | null {
  const source = isRecord(block.source) ? block.source : null;
  if (source === null) return null;
  if (source.type === 'base64' && typeof source.data === 'string') {
    const media = asString(source.media_type) !== '' ? asString(source.media_type) : 'image/png';
    return { type: 'image_url', image_url: { url: `data:${media};base64,${source.data}` } };
  }
  if (source.type === 'url' && typeof source.url === 'string') {
    return { type: 'image_url', image_url: { url: source.url } };
  }
  return null;
}

/** A tool_result block's payload as text: its text blocks, else its JSON. */
function toolResultText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    const joined = contentText(content);
    return joined !== '' ? joined : JSON.stringify(content);
  }
  return content === undefined ? '' : JSON.stringify(content);
}

/**
 * An Anthropic Messages request as a Chat Completions request naming `modelId`: the system prompt
 * becomes a system message, image blocks become image parts, tool_use blocks become tool_calls,
 * tool_result blocks become tool messages, and the output cap, sampling knobs, stop sequences, tools and
 * tool_choice pass through. `max_tokens` is defaulted rather than dropped — a request without an output
 * cap is refused upstream.
 */
function anthropicToOpenAI(body: Json, modelId: string): Json {
  const messages: Json[] = [];

  const system = contentText(body.system);
  if (system !== '') messages.push({ role: 'system', content: system });

  const source = Array.isArray(body.messages) ? body.messages : [];
  for (const raw of source) {
    if (!isRecord(raw)) continue;
    const fromAssistant = raw.role === 'assistant';
    const content = raw.content;

    if (typeof content === 'string') {
      messages.push({ role: fromAssistant ? 'assistant' : 'user', content });
      continue;
    }
    if (!Array.isArray(content)) continue;

    if (fromAssistant) {
      const calls: Json[] = [];
      for (const block of content) {
        if (!isRecord(block) || block.type !== 'tool_use') continue;
        calls.push({
          id: asString(block.id),
          type: 'function',
          function: {
            name: asString(block.name),
            arguments: JSON.stringify(isRecord(block.input) ? block.input : {}),
          },
        });
      }
      const text = contentText(content);
      messages.push({
        role: 'assistant',
        content: text !== '' ? text : null,
        ...(calls.length > 0 ? { tool_calls: calls } : {}),
      });
      continue;
    }

    const parts: Json[] = [];
    for (const block of content) {
      if (!isRecord(block)) continue;
      if (block.type === 'tool_result') {
        messages.push({
          role: 'tool',
          tool_call_id: asString(block.tool_use_id),
          content: toolResultText(block.content),
        });
        continue;
      }
      if (block.type === 'text' && typeof block.text === 'string') {
        parts.push({ type: 'text', text: block.text });
        continue;
      }
      const image = imagePart(block);
      if (image !== null) parts.push(image);
    }
    if (parts.length > 0) messages.push({ role: 'user', content: parts });
  }

  const out: Json = {
    model: modelId,
    messages,
    max_tokens: typeof body.max_tokens === 'number' ? body.max_tokens : DEFAULT_MAX_TOKENS,
  };
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) out.stop = body.stop_sequences;
  if (body.stream === true) out.stream = true;

  if (Array.isArray(body.tools)) {
    const tools = body.tools.flatMap((raw) => {
      if (!isRecord(raw) || asString(raw.name) === '') return [];
      return [
        {
          type: 'function',
          function: {
            name: asString(raw.name),
            description: asString(raw.description),
            parameters: isRecord(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} },
          },
        },
      ];
    });
    if (tools.length > 0) out.tools = tools;
  }

  const choice = body.tool_choice;
  if (isRecord(choice)) {
    if (choice.type === 'auto') out.tool_choice = 'auto';
    else if (choice.type === 'any') out.tool_choice = 'required';
    else if (choice.type === 'tool' && asString(choice.name) !== '') {
      out.tool_choice = { type: 'function', function: { name: asString(choice.name) } };
    }
  }

  return out;
}

/** An OpenAI request as it goes upstream: the client's body kept, the output cap defaulted. */
function openAIToOpenAI(body: Json, modelId: string): Json {
  const out: Json = { ...body, model: modelId };
  if (typeof out.max_tokens !== 'number') out.max_tokens = DEFAULT_MAX_TOKENS;
  if (body.stream === true) out.stream = true;
  return out;
}

// ───────────────────────────────────────────────── serve one prompt, with failover ──

/**
 * Serve one prompt: walk the ladder, dial each model, fall through on a refusal, and stop at the first
 * answer. Every refusal is recorded for the trail header, and the refusing model joins the cooling set so
 * a later prompt starts below it instead of spending its one call on the same 429.
 */
async function serve(requires: Requirement, buildBody: (modelId: string) => Json, wantsStream: boolean): Promise<Served | null> {
  const plan = planFor(requires);
  const failures: Failure[] = [];

  for (const row of plan) {
    let upstream: Response;
    try {
      upstream = await dial(row.id, buildBody(row.id));
    } catch (err) {
      // A timeout or a socket error is a refusal of this model for this run, not of the prompt.
      failures.push({ status: 504, code: `dial_${(err as Error).name || 'error'}`.slice(0, MAX_FAILURE_CODE), model: row.id });
      cooling.add(row.id);
      continue;
    }

    if (!upstream.ok || upstream.body === null) {
      const body = await upstream.text().catch(() => '');
      failures.push({ status: upstream.status, code: refusalCode(upstream.status, body), model: row.id });
      cooling.add(row.id);
      continue;
    }

    try {
      const answer = wantsStream ? await readStream(upstream.body) : readJsonAnswer(await upstream.json());
      // A 200 carrying nothing is not an answer: the prompt's token cannot be in an empty body, so the
      // prompt would be counted as unserved and earn nothing. Treat it as a refusal of this model and let
      // the ladder try the next one — an empty reply costs exactly as much as a 4xx and buys even less.
      if (answer.text.trim() === '' && answer.toolCalls.length === 0) {
        failures.push({ status: 502, code: 'empty_answer', model: row.id });
        continue;
      }
      return { answer, model: row.id, failures };
    } catch (err) {
      failures.push({ status: 502, code: `read_${(err as Error).name || 'error'}`.slice(0, MAX_FAILURE_CODE), model: row.id });
      cooling.add(row.id);
    }
  }

  return null;
}

// ─────────────────────────────────────────────────────────────────── routes ──

/** One client request body, or null when it is not one JSON object or is over the cap. */
async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk));
    size += buffer.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(buffer);
  }
  if (chunks.length === 0) return null;
  try {
    const parsed: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown, extra: Record<string, string> = {}): void {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(text),
    ...extra,
  });
  res.end(text);
}

function sendSse(res: ServerResponse, frames: readonly string[], extra: Record<string, string> = {}): void {
  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
    ...extra,
  });
  for (const frame of frames) res.write(frame);
  res.end();
}

function openAIError(message: string, type: string, code: string | null): Json {
  return { error: { message, type, code } };
}

function anthropicError(type: string, message: string): Json {
  return { type: 'error', error: { type, message } };
}

function fail(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string): void {
  if (wire === 'anthropic') sendJson(res, status, anthropicError(type, message));
  else sendJson(res, status, openAIError(message, type, null));
}

/** The headers a served prompt carries: the failure trail, when there is one to declare. */
function trailHeaders(served: Served): Record<string, string> {
  const trail = failureTrail(served.failures);
  return trail === '' ? {} : { 'x-makefun-upstream-failures': trail };
}

/** GET /v1/models, in the shape of the auth header: x-api-key means Anthropic, else OpenAI. */
function modelsRoute(req: IncomingMessage, res: ServerResponse): void {
  const ids = catalog.providers.flatMap((provider) => provider.models.map((model) => ({ id: model.id, provider: provider.id })));
  if (typeof req.headers['x-api-key'] === 'string') {
    sendJson(res, 200, {
      data: ids.map((row) => ({ type: 'model', id: row.id, display_name: row.id, created_at: FIXED_CREATED_AT })),
      has_more: false,
    });
    return;
  }
  sendJson(res, 200, {
    object: 'list',
    data: ids.map((row) => ({ id: row.id, object: 'model', created: FIXED_CREATED, owned_by: row.provider })),
  });
}

/** POST /v1/chat/completions — an OpenAI-shaped client. */
async function chatRoute(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return fail(res, 'openai', 400, 'invalid_request_error', 'the request body is not one JSON object');

  const requires = requirementsFor(body);
  const wantsStream = body.stream === true;
  const served = await serve(requires, (modelId) => openAIToOpenAI(body, modelId), wantsStream);

  if (served === null) {
    return fail(res, 'openai', 502, 'upstream_error', 'every free model that meets this request refused it');
  }
  if (wantsStream) return sendSse(res, openAIStream(served), trailHeaders(served));
  sendJson(res, 200, openAIJson(served), trailHeaders(served));
}

/** POST /v1/messages — an Anthropic-shaped client, translated both ways. */
async function messagesRoute(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const body = await readJson(req);
  if (body === null) return fail(res, 'anthropic', 400, 'invalid_request_error', 'the request body is not one JSON object');

  const requires = requirementsFor(body);
  const wantsStream = body.stream === true;
  const served = await serve(requires, (modelId) => anthropicToOpenAI(body, modelId), wantsStream);

  if (served === null) {
    return fail(res, 'anthropic', 502, 'api_error', 'every free model that meets this request refused it');
  }
  if (wantsStream) return sendSse(res, anthropicStream(served), trailHeaders(served));
  sendJson(res, 200, anthropicJson(served), trailHeaders(served));
}

async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') return modelsRoute(req, res);
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') return chatRoute(req, res);
  if (req.method === 'POST' && url.pathname === '/v1/messages') return messagesRoute(req, res);
  const wire = url.pathname === '/v1/messages' ? 'anthropic' : 'openai';
  return fail(res, wire, 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}

// ───────────────────────────────────────────────────────── register and poll ──

function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '0.0.0.0', () => resolve());
  });
}

/** POST /register with the port and the one catalog; the env answers with the trial's prompt count. */
async function register(port: number): Promise<void> {
  const res = await fetch(`${ENV_URL}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog }),
    signal: AbortSignal.timeout(5000),
  });
  if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
  const reply: unknown = await res.json();
  if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the trial holds ${reply.requests} prompts`);
}

/** Poll GET /work every 100 ms and exit 0 the moment it says done: nothing else may hold the process. */
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) {
          clearInterval(timer);
          server.close();
          process.exit(0);
        }
      } catch {
        // The env is not up yet: the next tick tries again.
      } finally {
        inFlight = false;
      }
    })();
  }, POLL_INTERVAL_MS);
}

async function start(): Promise<void> {
  if (brief.error !== null) log(brief.error);

  if (process.argv.includes('--list')) {
    // The same document the register body carries, byte for byte: print it once and stop before listening.
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }

  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  if (chatUrl() === '') throw new Error('OPENAI_BASE_URL is not set');

  const server = createServer((req, res) => {
    void handle(req, res).catch((err: unknown) => {
      log((err as Error).stack ?? String(err));
      if (!res.headersSent) {
        const wire = req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai';
        fail(res, wire, 500, 'internal_error', 'the server failed to answer');
      }
    });
  });

  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  // 8080 is the env's on one layout; the ephemeral range never holds it, and the guard says so loudly.
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');

  const listed = catalog.providers.reduce((total, provider) => total + provider.models.length, 0);
  log(`listening on 0.0.0.0:${port} with ${listed} free models and ${brief.prompts.length} prompt rows`);
  await register(port);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}

try {
  await start();
} catch (err) {
  log((err as Error).stack ?? String(err));
  process.exit(1);
}