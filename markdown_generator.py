from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

class CustomMarkdownGenerator(DefaultMarkdownGenerator):
    def generate_markdown(self, data):
        # Custom logic to format the Markdown output
        return f"## {data['title']}\n\n{data['content']}"
