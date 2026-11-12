# custom-operators

Shared Lakeflow Designer custom operators. Each operator is one YAML file in `operators/`.

* `tests/test_operators.py` loads each file and runs its logic on rows with known answers.
* The `run_tests` notebook runs the same tests in the workspace.
* `.github/workflows/operator-tests.yml` runs them automatically on every pull request.
