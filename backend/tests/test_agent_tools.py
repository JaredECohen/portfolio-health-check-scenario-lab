from app.tools.agent_tools import summarize_text_nlp


def test_summarize_text_nlp_extracts_deterministic_signals() -> None:
    summary = summarize_text_nlp(
        """
        Apple reported strong demand and improved margins. Tim Cook said the outlook remains strong.
        The company also noted some cautious language around China demand and regulatory risk.
        Apple expects supply improvements to support volume growth.
        """
    )

    assert summary.sentiment_counts["positive"] >= 3
    assert summary.sentiment_counts["cautious"] >= 2
    assert "demand" in summary.keywords
    assert any(entity.entity == "Apple" for entity in summary.entities)
    assert any(cluster.topic == "guidance" for cluster in summary.topic_clusters)
