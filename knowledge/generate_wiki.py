import anthropic
import os

client = anthropic.Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

# Read all raw files
raw_content = ''
for fname in sorted(os.listdir('knowledge/raw/')):
    if fname.endswith('.md'):
        with open(f'knowledge/raw/{fname}') as f:
            raw_content += f'\n\n=== {fname} ===\n' + f.read()[:2000]

# Generate overview wiki
def generate_wiki(prompt, filename):
    msg = client.messages.create(
        model='claude-opus-4-5',
        max_tokens=2000,
        messages=[{'role': 'user', 'content': prompt + '\n\nSOURCES:\n' + raw_content[:15000]}]
    )
    os.makedirs('knowledge/wiki', exist_ok=True)
    with open(f'knowledge/wiki/{filename}', 'w') as f:
        f.write(msg.content[0].text)
    print(f'Generated {filename}')

generate_wiki('''You are a marketing analytics expert. Based on these sources about Paramount+, write a comprehensive wiki overview page covering: company overview, streaming strategy, subscriber growth, competitive position, and key business metrics. Format as clean markdown.''', 'overview.md')

generate_wiki('''You are a marketing analytics expert. Based on these sources about Paramount+, write a wiki page about key entities: important executives, major content franchises, key partnerships, and competitor services. Format as clean markdown.''', 'key_entities.md')

generate_wiki('''You are a marketing analytics expert. Based on these sources about Paramount+, write a wiki page identifying the major themes in Paramount+ strategy: content themes, marketing themes, audience targeting themes, and paid social implications for a marketing analyst. Format as clean markdown.''', 'themes.md')

print('All wiki pages generated!')
