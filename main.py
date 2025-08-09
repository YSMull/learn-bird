from openai import OpenAI

client = OpenAI(
    api_key="lm-studio",
    base_url="http://localhost:1234/v1",
)

if __name__ == "__main__":
    stream_response  = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        stream=True,
        messages=[
            {
                "role": "user",
                "content": "Hello, world!"
            }
        ]
    )
    collected_chunks = []
    for chunk in stream_response:
        if chunk.choices[0].delta.content:
            # Collect the content of the chunk
            if chunk.choices and chunk.choices[0].delta.content:
                collected_chunks.append(chunk.choices[0].delta.content)
    print("".join(collected_chunks))