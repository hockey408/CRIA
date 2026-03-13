import anthropic

client = anthropic.Anthropic()  # automatically picks up env variable

response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=500,
    messages=[
        {"role": "user", "content": "What are the top 3 risk factors in commercial real estate lending right now?"}
    ]
)

print(response.content[0].text)