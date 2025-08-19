#!/usr/bin/env python3
"""
Markdown Testing Playground
Test different markdown configurations and formatting options
"""

import markdown
from markdown.extensions import codehilite, fenced_code, tables, toc
from markupsafe import Markup

# Sample text that causes problems
test_text = """

### Detailed Recommendations

#### 1. Index: `IX_Product_Color_Included`
- **Reason for Removal**: This index is not used for any reads (0 seeks, scans, or lookups). Its functionality is covered by `IX_Product_Color_SafetyStock_ReorderPoint` and `IX_Product_ListPrice_Included`.
- **Covering Indexes**:
  - `IX_Product_Color_SafetyStock_ReorderPoint`
  - `IX_Product_ListPrice_Included`
- **SQL Command**:
  ```sql
  DROP INDEX IX_Product_Color_Included ON Production.Product;
  ```
- **Recommendation Link**: [View Recommendation](http://localhost:5001/recommendation/1)

#### 2. Another Index Example
Some more content here with **bold text** and `inline code`.

### Actionable Recommendations

The following recommendations have been created for each specific index removal:

1. [Remove redundant index IX_Product_Color_Included on Production.Product - functionality covered by IX_Product_Color_SafetyStock_ReorderPoint and IX_Product_ListPrice_Included](http://localhost:5001/recommendation/1)
   - SQL: `DROP INDEX IX_Product_Color_Included ON Production.Product;`

### Summary
This is a summary section with:
1. Numbered item one
2. Numbered item two
   - Sub-bullet point
   - Another sub-bullet
3. Final numbered item
"""

def test_basic_markdown():
    """Test basic markdown without extensions"""
    print("=" * 60)
    print("BASIC MARKDOWN (no extensions)")
    print("=" * 60)
    result = markdown.markdown(test_text)
    print(result)
    print("\n")

def test_markdown_with_extensions():
    """Test markdown with various extensions"""
    print("=" * 60)
    print("MARKDOWN WITH EXTENSIONS")
    print("=" * 60)

    extensions = ['fenced_code', 'tables', 'codehilite', 'toc']
    extension_configs = {
        'codehilite': {
            'css_class': 'highlight',
            'use_pygments': False
        }
    }

    result = markdown.markdown(
        test_text,
        extensions=extensions,
        extension_configs=extension_configs
    )
    print(result)
    print("\n")

def test_custom_simple_formatter():
    """Test our own simple formatter"""
    print("=" * 60)
    print("CUSTOM SIMPLE FORMATTER")
    print("=" * 60)

    import re
    text = test_text

    # Convert ### Header to <h3>Header</h3>
    text = re.sub(r'^#### (.+)$', r'<h4>\1</h4>', text, flags=re.MULTILINE)
    text = re.sub(r'^### (.+)$', r'<h3>\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r'^## (.+)$', r'<h2>\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r'^# (.+)$', r'<h1>\1</h1>', text, flags=re.MULTILINE)

    # Convert **bold** to <strong>bold</strong>
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)

    # Convert `code` to <code>code</code>
    text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)

    # Convert [text](url) to <a href="url">text</a>
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)

    # Handle code blocks ```lang ... ```
    text = re.sub(r'```(\w+)?\n(.*?)\n```', r'<pre><code>\2</code></pre>', text, flags=re.DOTALL)

    # Convert line breaks to <br> but preserve list structure
    lines = text.split('\n')
    formatted_lines = []
    for line in lines:
        if line.strip().startswith('- ') or line.strip().startswith('  - ') or re.match(r'^\d+\. ', line.strip()):
            # Keep list items as they are
            formatted_lines.append(line)
        else:
            formatted_lines.append(line)

    text = '\n'.join(formatted_lines)

    # Convert newlines to <br> except around HTML tags
    text = re.sub(r'\n(?!<)', '<br>\n', text)

    print(text)
    print("\n")

def test_markdown_minimal_config():
    """Test markdown with minimal but useful configuration"""
    print("=" * 60)
    print("MARKDOWN WITH MINIMAL CONFIG")
    print("=" * 60)

    # Only enable extensions that help with lists and code
    extensions = ['fenced_code']

    result = markdown.markdown(
        test_text,
        extensions=extensions,
        tab_length=4
    )
    print(result)
    print("\n")

def test_markdown_preserve_structure():
    """Test markdown with configuration to preserve list structure"""
    print("=" * 60)
    print("MARKDOWN PRESERVE STRUCTURE")
    print("=" * 60)

    # Try with different configurations
    md = markdown.Markdown(
        extensions=['fenced_code', 'tables'],
        tab_length=2,
        output_format='html'
    )

    result = md.convert(test_text)
    print(result)
    print("\n")

def interactive_test():
    """Interactive testing mode"""
    print("=" * 60)
    print("INTERACTIVE MODE")
    print("=" * 60)
    print("Enter your markdown text (press Ctrl+D or empty line to finish):")

    lines = []
    try:
        while True:
            line = input()
            if not line:  # Empty line ends input
                break
            lines.append(line)
    except EOFError:
        pass

    if not lines:
        print("No input provided.")
        return

    user_text = '\n'.join(lines)

    print("\n" + "=" * 40)
    print("YOUR INPUT:")
    print("=" * 40)
    print(user_text)

    print("\n" + "=" * 40)
    print("BASIC MARKDOWN OUTPUT:")
    print("=" * 40)
    print(markdown.markdown(user_text))

    print("\n" + "=" * 40)
    print("WITH EXTENSIONS:")
    print("=" * 40)
    result = markdown.markdown(user_text, extensions=['fenced_code', 'tables'])
    print(result)

def generate_html_report():
    """Generate HTML report with all test results"""
    print("Generating HTML report...")

    # CSS styles as a separate string to avoid format conflicts
    css_styles = """
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }
        .test-section {
            border: 2px solid #ddd;
            margin: 20px 0;
            padding: 20px;
            border-radius: 8px;
            background-color: #f9f9f9;
        }
        .test-title {
            background-color: #007BFF;
            color: white;
            padding: 10px;
            margin: -20px -20px 20px -20px;
            border-radius: 6px 6px 0 0;
            font-size: 1.2em;
            font-weight: bold;
        }
        .original-markdown {
            background-color: #f4f4f4;
            border: 1px solid #ccc;
            border-radius: 4px;
            padding: 15px;
            margin: 10px 0;
            font-family: 'Courier New', monospace;
            white-space: pre-wrap;
        }
        .result {
            background-color: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 15px;
            margin: 10px 0;
        }
        h1, h2, h3, h4 {
            color: #333;
        }
        code {
            background-color: #f4f4f4;
            border: 1px solid #ddd;
            border-radius: 3px;
            padding: 2px 4px;
            font-family: 'Courier New', monospace;
        }
        pre {
            background-color: #f4f4f4;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            overflow-x: auto;
        }
        .nav {
            background-color: #333;
            padding: 10px 0;
            margin: -20px -20px 20px -20px;
        }
        .nav a {
            color: white;
            text-decoration: none;
            margin: 0 15px;
            padding: 5px 10px;
            border-radius: 3px;
        }
        .nav a:hover {
            background-color: #555;
        }
    """

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Markdown Testing Results</title>
    <style>{css_styles}</style>
</head>
<body>
    <h1>Markdown Testing Results</h1>

    <div class="nav">
        <a href="#original">Original Text</a>
        <a href="#basic">Basic Markdown</a>
        <a href="#extensions">With Extensions</a>
        <a href="#custom">Custom Formatter</a>
        <a href="#minimal">Minimal Config</a>
        <a href="#preserve">Preserve Structure</a>
    </div>

    <div id="original" class="test-section">
        <div class="test-title">Original Markdown Text</div>
        <div class="original-markdown">{test_text.replace('<', '&lt;').replace('>', '&gt;')}</div>
    </div>
"""

    # Test 1: Basic markdown
    result1 = markdown.markdown(test_text)
    html_content += f"""
    <div id="basic" class="test-section">
        <div class="test-title">1. Basic Markdown (no extensions)</div>
        <div class="result">{result1}</div>
    </div>
"""

    # Test 2: With extensions
    extensions = ['fenced_code', 'tables', 'codehilite', 'toc']
    extension_configs = {
        'codehilite': {
            'css_class': 'highlight',
            'use_pygments': False
        }
    }
    result2 = markdown.markdown(
        test_text,
        extensions=extensions,
        extension_configs=extension_configs
    )
    html_content += f"""
    <div id="extensions" class="test-section">
        <div class="test-title">2. Markdown with Extensions (fenced_code, tables, codehilite, toc)</div>
        <div class="result">{result2}</div>
    </div>
"""

    # Test 3: Custom formatter
    import re
    text = test_text
    text = re.sub(r'^#### (.+)$', r'<h4>\1</h4>', text, flags=re.MULTILINE)
    text = re.sub(r'^### (.+)$', r'<h3>\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r'^## (.+)$', r'<h2>\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r'^# (.+)$', r'<h1>\1</h1>', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
    text = re.sub(r'```(\w+)?\n(.*?)\n```', r'<pre><code>\2</code></pre>', text, flags=re.DOTALL)
    text = re.sub(r'\n(?!<)', '<br>\n', text)
    result3 = text

    html_content += f"""
    <div id="custom" class="test-section">
        <div class="test-title">3. Custom Simple Formatter</div>
        <div class="result">{result3}</div>
    </div>
"""

    # Test 4: Minimal config
    result4 = markdown.markdown(test_text, extensions=['fenced_code'], tab_length=4)
    html_content += f"""
    <div id="minimal" class="test-section">
        <div class="test-title">4. Minimal Config (fenced_code only)</div>
        <div class="result">{result4}</div>
    </div>
"""

    # Test 5: Preserve structure
    md = markdown.Markdown(
        extensions=['fenced_code', 'tables'],
        tab_length=2,
        output_format='html'
    )
    result5 = md.convert(test_text)
    html_content += f"""
    <div id="preserve" class="test-section">
        <div class="test-title">5. Preserve Structure Config</div>
        <div class="result">{result5}</div>
    </div>
"""

    html_content += """
</body>
</html>
"""

    # Write to file
    with open('markdown_test_results.html', 'w', encoding='utf-8') as f:
        f.write(html_content)

    print("HTML report generated: markdown_test_results.html")
    print("Open this file in your browser to see the results!")

def main():
    """Main function to run all tests"""
    print("MARKDOWN TESTING PLAYGROUND")
    print("==========================")
    print()

    while True:
        print("Choose test option:")
        print("1. Basic markdown")
        print("2. Markdown with extensions")
        print("3. Custom simple formatter")
        print("4. Minimal config")
        print("5. Preserve structure")
        print("6. Interactive test")
        print("7. Run all tests")
        print("8. Generate HTML report")
        print("0. Exit")

        choice = input("\nEnter choice (0-8): ").strip()

        if choice == '0':
            break
        elif choice == '1':
            test_basic_markdown()
        elif choice == '2':
            test_markdown_with_extensions()
        elif choice == '3':
            test_custom_simple_formatter()
        elif choice == '4':
            test_markdown_minimal_config()
        elif choice == '5':
            test_markdown_preserve_structure()
        elif choice == '6':
            interactive_test()
        elif choice == '7':
            test_basic_markdown()
            test_markdown_with_extensions()
            test_custom_simple_formatter()
            test_markdown_minimal_config()
            test_markdown_preserve_structure()
        elif choice == '8':
            generate_html_report()
        else:
            print("Invalid choice. Please try again.")

        input("\nPress Enter to continue...")
        print("\n" * 2)

if __name__ == "__main__":
    main()
