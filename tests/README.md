# Function Tests

This directory contains test files for the `ReanudarRutaFn` Lambda function.

## Test Suite (4 Mandatory Test Types)

Every Lambda function must pass a **test suite** before publishing. The suite consists of up to 4 test types:

| Test Type | File | Required? | Purpose |
|-----------|------|-----------|---------|
| **Provided Params** | `test_provided_params.json` | Always | Test with explicit parameters from manifest |
| **Operator Params** | `test_operator_params.json` | Always | Simulate operator LLM generating params from a prompt |
| **File Handling** | `test_file_handling.json` | If `testing.handles_files: true` | Test file upload/download/processing |
| **Widget Data** | `test_widget_data.json` | If `widget.enabled: true` | Test widget data extraction from pipeline node |

### Generating Test Files

```bash
# Generate all applicable test files from manifest.yml
chask function test:init --suite

# Generate a single legacy test file
chask function test:init
```

### Running the Test Suite

```bash
# Run all applicable tests (creates suite, runs each type, reports gate status)
chask function test:suite

# Run only a specific test type
chask function test:suite --type provided_params

# Run a single legacy test file
chask function test tests/test_basic.json
```

### Publish Gating

`chask function publish` checks the test gate before deploying. All required tests must pass for the current commit SHA. Use `--skip-tests` to bypass (not recommended).

```bash
# Normal publish (checks test gate)
chask function publish

# Bypass gate (emergency only)
chask function publish --skip-tests
```

## Test File Structure

```json
{
  "function_name": "ReanudarRutaFn",
  "test_type": "provided_params",
  "args": {
    "param1": "value1"
  },
  "widget_params": {},
  "prompt": "",
  "event_type": "function_call",
  "extra_params": {},
  "files": [],
  "metadata": {
    "description": "Description of this test case",
    "expected_status": "success"
  }
}
```

### Fields

- **function_name**: Must match manifest.yml `function.name`
- **test_type**: One of `provided_params`, `operator_params`, `file_handling`, `widget_data`
- **args**: Function parameters (from `tool_calls[0].args`)
- **widget_params**: Widget/secret parameters (API keys, tokens)
- **prompt**: Custom prompt for the orchestration event
- **event_type**: Orchestration event type (default: `function_call`)
- **extra_params**: Additional config (e.g., `openai_api_key`, `model`)
- **files**: Array of file paths from `test_files/` directory
- **metadata.description**: Human-readable test case description
- **metadata.expected_status**: Expected outcome (`success` or `failure`)

### Test Type Details

#### Provided Params (`test_provided_params.json`)
Test the function with explicitly defined parameters. Fill `args` with valid values for all required parameters from `manifest.yml`.

#### Operator Params (`test_operator_params.json`)
Simulate the operator LLM generating parameters. Provide a `prompt` describing what the user wants, and set `extra_params.is_operator_params_test: true`. The orchestrator will invoke the operator first, then use the generated params to call your function.

#### File Handling (`test_file_handling.json`)
Test file operations. Place test files in `test_files/` and reference them in the `files` array. The test infrastructure uploads them and injects UUIDs into the function args.

#### Widget Data (`test_widget_data.json`)
Test widget parameter extraction. Fill `widget_params` with values matching your manifest's `widget.params` configuration.

## Tenant Data Tests

If your Lambda uses `TenantDataClient`, declare `function.data` in `manifest.yml`, add idempotent SQL files under `migrations/`, and include at least one provided-params test that exercises the tenant-data path. Test arguments belong in `args`; function code should read them through `_extract_tool_args()`, which maps to `extra_params.tool_calls[0].args` during real orchestration.

## Test Files Directory

Place any input files needed for testing in the `test_files/` directory:

```
test_files/
  sample_input.csv
  test_document.pdf
  .gitkeep
```
