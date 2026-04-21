Thank you for your interest in contributing to Options Whale Tracker!

Quick guide

- Fork the repository and create a feature branch from `main`:
  `git checkout -b feat/short-description`
- Run the app locally and ensure your change works:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

- Keep changes focused and add tests where appropriate.
- Open a pull request to `main` with a clear description and screenshots if UI changes.

Code style

- Follow existing project formatting (PEP8). The repo includes simple linting in CI (if configured).

Commit message guidelines

- Use conventional commits style briefly, e.g. `feat: add moneyness filter` or `docs: update README`.

Security

- Do not commit secrets (API keys, tokens). Use `.env` and ensure it is listed in `.gitignore`.

Questions

- Open an issue on GitHub and tag it with `question` or `discussion`.

