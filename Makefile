.PHONY: fmt lint build serve links

# Форматировать все Markdown файлы
fmt:
	uvx --with mdformat-mkdocs --with mdformat-gfm mdformat docs/

# Проверить форматирование (без изменений)
lint:
	uvx --with mdformat-mkdocs --with mdformat-gfm mdformat --check docs/

# Проверить все внешние ссылки
links:
	@find docs -name '*.md' | while read f; do \
		npx -y markdown-link-check --config .markdown-link-check.json "$$f"; \
	done

# Собрать сайт
build:
	uvx --with mkdocs-material mkdocs build

# Локальный сервер
serve:
	uvx --with mkdocs-material mkdocs serve
