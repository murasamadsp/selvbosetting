.PHONY: fmt lint build serve

# Форматировать все Markdown файлы
fmt:
	uvx --with mdformat-mkdocs --with mdformat-gfm mdformat docs/

# Проверить форматирование (без изменений)
lint:
	uvx --with mdformat-mkdocs --with mdformat-gfm mdformat --check docs/

# Собрать сайт
build:
	uvx --with mkdocs-material mkdocs build

# Локальный сервер
serve:
	uvx --with mkdocs-material mkdocs serve
