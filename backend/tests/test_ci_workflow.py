from pathlib import Path


def test_ci_workflow_triggers_cover_feature_branches():
    workflow = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
    content = workflow.read_text(encoding="utf-8")

    assert "push:" in content
    assert "pull_request:" in content
    assert "branches: [main, master, develop]" not in content
