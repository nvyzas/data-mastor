<!--
Guidance for AI coding assistants working on this repository.
Keep this short (20-50 lines) and focused on repository-specific patterns, commands, and files to inspect. Update when project conventions change.
-->

---------------
github_owner: nvyzas
github_repo: data-mastor
assistant: true
parseable: true
preferred_tools:
  - files
  - terminal
  - github
  - tests
disallowed_tools:
  - playwright mcp (not needed)
---------------

# Data Mastor — AI assistant quick instructions

- Big picture: this repo is a collection of utilities that help in data-science workflows. The main utility currently offered (and primarily focused) is the scraper package under `src/data_mastor/scraper`.

# Scraper (package)

## spiders (module)

## middlewares (module)

## pipelines (module)

# Tooling

## uv
Agents should run all other tooling commands by prepending 'uv run' to the actual command

## pytest
Agents should make sure all tests pass before commiting by running: 'pytest -q'

## ruff
Agents should make sure everything is properly formatted and has no errors before commiting by running: 'ruff check --fix'
