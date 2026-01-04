"""
Unit tests for topic taxonomy validation and matching behavior.
"""

from mpreg.core.topic_taxonomy import TopicAccessLevel, TopicPattern, TopicValidator


class TestTopicValidator:
    """Validate wildcard rules and matching semantics."""

    def test_validate_topic_pattern_star_segment_rules(self) -> None:
        """Ensure * is only allowed as a full segment."""
        is_valid, error_msg = TopicValidator.validate_topic_pattern("user.*.events")
        assert is_valid
        assert not error_msg

        is_valid, error_msg = TopicValidator.validate_topic_pattern("user.a*.events")
        assert not is_valid
        assert error_msg

        is_valid, error_msg = TopicValidator.validate_topic_pattern("user.*suffix")
        assert not is_valid
        assert error_msg

    def test_matches_pattern_with_mid_hash(self) -> None:
        """Ensure # matches zero or more segments in any position."""
        assert TopicValidator.matches_pattern("user.events", "user.#.events")
        assert TopicValidator.matches_pattern("user.login.events", "user.#.events")
        assert TopicValidator.matches_pattern("user.login.deep.events", "user.#.events")
        assert not TopicValidator.matches_pattern("admin.login.events", "user.#.events")


class TestTopicPattern:
    """Validate TopicPattern uses the shared matcher."""

    def test_topic_pattern_matches_topic(self) -> None:
        pattern = TopicPattern(
            pattern="user.#.events",
            access_level=TopicAccessLevel.DATA_PLANE,
            description="User events",
        )

        assert pattern.matches_topic("user.events")
        assert pattern.matches_topic("user.login.events")
        assert not pattern.matches_topic("admin.login.events")
