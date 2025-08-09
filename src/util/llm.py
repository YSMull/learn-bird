from openai import OpenAI

client = OpenAI(
    api_key="lm-studio",
    base_url="http://localhost:1234/v1",
)


def llm(prompt, model="openai/gpt-oss-20b", max_tokens=1000, temperature=0.7):
    stream_response = client.chat.completions.create(
        model=model,
        stream=True,
        max_tokens=max_tokens,
        temperature=temperature,
        messages=[{"role": "user", "content": prompt}],
    )
    collected_chunks = []
    print("")
    for chunk in stream_response:
        if chunk.choices and chunk.choices[0].delta.content:
            collected_chunks.append(chunk.choices[0].delta.content)
            # print(chunk.choices[0].delta.content, end="", flush=True)
    return "".join(collected_chunks)
