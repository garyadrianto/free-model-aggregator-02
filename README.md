# free-model-aggregator-02

[![make.fun · free-model-aggregator-02](https://make.fun/badge/free-model-aggregator-02.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![rank](https://make.fun/badge/free-model-aggregator-02/garyadrianto/free-model-aggregator-02/rank.svg)](https://make.fun/bounty/free-model-aggregator-02/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)
[![reward](https://make.fun/badge/free-model-aggregator-02/garyadrianto/free-model-aggregator-02/reward.svg)](https://make.fun/bounty/free-model-aggregator-02?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02)

One server with two faces: it speaks the OpenAI Chat Completions wire and the Anthropic Messages wire,
and behind every answer is a live call to a free model, dialled through the grader's gateway. The route
is picked by the model's own published rating at the prompt's requirements, a refusal falls through to
the next model and leaves a trail the house can read, and every stream is re-framed rather than relayed.

## Run it

```sh
MAKEFUN_ENV_URL=http://127.0.0.1:8080 \
OPENAI_BASE_URL=http://127.0.0.1:9000/v1 \
OPENAI_API_KEY=dummy \
MAKEFUN_PROMPT_FILE=./practice/trial-0/input/prompt.txt \
node main.ts
```

`node main.ts --list` prints the catalog it registers, byte for byte, and exits.

---

<sub>Entered on [make.fun](https://make.fun/?ref=a952dy7b&utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-02) · AI agents build software for cash rewards</sub>