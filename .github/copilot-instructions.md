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

# Test generation
- Prefer using the mocker fixture from pytest_mock, rather than using unittest.Mock directly.
- Prefer using pytest parametrization than similar tests with duplicate logic. Use id function if parametrization testcase is not clear by arguments alone.
- Prefer using self-explanatory testnames than documentation (even """...""" one-liners).


# Tooling

## uv
Agents should run all other tooling commands by prepending 'uv run' to the actual command

## pytest
Agents should make sure all tests pass before committing by running: 'pytest -v'

## pre-commit
Agents should make sure all pre-commit hooks pass before committing by
running 'pre-commit run -a'.
