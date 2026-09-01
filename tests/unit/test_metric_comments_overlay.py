"""Unit tests for the metric-comment chart overlay (Phase C).

Covers ``GET /api/v1/signals/{signal_id}/metric-comments`` - the side fetch a
chart makes to get the annotations for the window it is drawing.  The route
lives on the SIGNALS router but is gated by ``require_access("comments")``,
the same category the rest of the metric-comment surface uses.

The window and metric-type rules live in the WHERE clause, so asserting only
on the HTTP status would test nothing.  Two of these tests therefore capture
the statement the endpoint issued - the same technique
``tests/unit/test_settings_api.py`` uses to prove an ORDER BY - and evaluate
its WHERE clause against fixture rows in stdlib sqlite3.  A row is "matched"
when the endpoint's own predicate selects it, so include/exclude is decided
by the production code rather than restated by the test.
"""

import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI, status
from fastapi.testclient import TestClient
from sqlalchemy.dialects import sqlite as sqlite_dialect

from tests._helpers import make_mock_session
from tsigma.api.v1.signals import router as signals_router
from tsigma.auth.dependencies import _get_db_session, get_current_user_optional
from tsigma.auth.sessions import SessionData
from tsigma.dependencies import get_audited_session, get_session
from tsigma.settings_service import settings_cache

PREFIX = '/api/v1/signals'
SIGNAL_ID = 'SIG-1'
OTHER_SIGNAL_ID = 'SIG-2'

AUTHOR_ID = uuid4()
READER_ID = uuid4()

# The parent signal get_or_404 finds; pass ``signal=None`` for the 404 case.
SIGNAL_ROW = SimpleNamespace(signal_id=SIGNAL_ID)

# The chart window every window test asks for.
WINDOW_START = datetime(2023, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2023, 1, 20, tzinfo=timezone.utc)


def _user(user_id, username, role='viewer'):
    return SessionData(
        user_id=user_id, username=username, role=role,
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def _reader():
    return _user(READER_ID, 'bob')


def _mk_app():
    app = FastAPI()
    app.include_router(signals_router, prefix=PREFIX)
    return app


signals_app = _mk_app()


def _overrides(app, mock_session, user, policy_rows=()):
    """Wire auth + session dependency overrides. ``user`` of None means unauthenticated."""
    app.dependency_overrides[get_current_user_optional] = (lambda: user)

    async def _access_db():
        m = AsyncMock()
        r = MagicMock()
        r.scalars.return_value.all.return_value = list(policy_rows)
        m.execute = AsyncMock(return_value=r)
        yield m

    app.dependency_overrides[_get_db_session] = _access_db

    async def _sess():
        yield mock_session

    app.dependency_overrides[get_session] = _sess
    app.dependency_overrides[get_audited_session] = _sess

    # require_access caches the policy table; without this the previous
    # test's 'comments' policy leaks into this one.
    settings_cache.invalidate()


def _result(scalar=None, items=None):
    r = MagicMock()
    items = list(items or [])
    r.scalar_one_or_none.return_value = scalar
    r.scalar.return_value = scalar
    r.scalars.return_value.all.return_value = items
    r.scalars.return_value.first.return_value = items[0] if items else None
    return r


def _session(signal=SIGNAL_ROW, comments=()):
    """A mock session answering the signal lookup, then the comment select."""
    mock_session = make_mock_session()
    signal_result = _result(scalar=signal, items=[signal] if signal else [])
    comment_result = _result(items=list(comments))

    def _execute(statement, *args, **kwargs):
        if 'FROM config.metric_comment' in str(statement):
            return comment_result
        return signal_result

    mock_session.execute = AsyncMock(side_effect=_execute)
    return mock_session


def _comment_obj(comment_id=None, anchor_start=None, anchor_end=None, text='Construction here'):
    now = datetime.now(timezone.utc)
    return MagicMock(
        id=comment_id or uuid4(),
        signal_id=SIGNAL_ID,
        text=text,
        author_uuid=AUTHOR_ID,
        author_username='alice',
        anchor_start=anchor_start,
        anchor_end=anchor_end,
        created_at=now,
        updated_at=now,
    )


def _client(mock_session, user, policy_rows=()):
    _overrides(signals_app, mock_session, user, policy_rows)
    return TestClient(signals_app)


def _get(mock_session, user, signal_id=SIGNAL_ID, params=None, policy_rows=()):
    return _client(mock_session, user, policy_rows).get(
        f'{PREFIX}/{signal_id}/metric-comments', params=params or {}
    )


def _comment_stmt(mock_session):
    """The SELECT the endpoint issued against metric_comment."""
    for call in mock_session.execute.await_args_list:
        statement = call.args[0]
        if 'FROM config.metric_comment' in str(statement):
            return statement
    raise AssertionError('the endpoint never queried metric_comment')


# ---------------------------------------------------------------------------
# Predicate fixtures - evaluated through the endpoint's own WHERE clause
# ---------------------------------------------------------------------------

def _at(day, hour=0):
    return datetime(2023, 1, day, hour, tzinfo=timezone.utc)


# (label, signal_id, anchor_start, anchor_end, metric type keys)
# Exactly three anchor states are represented; a fourth (anchor_end with no
# anchor_start) is barred by a CheckConstraint and cannot be stored.
PREDICATE_ROWS = (
    ('unanchored', SIGNAL_ID, None, None, ()),
    ('point_inside', SIGNAL_ID, _at(15), None, ('ApproachDelay',)),
    ('point_at_start', SIGNAL_ID, _at(10), None, ()),
    ('point_at_end', SIGNAL_ID, _at(20), None, ()),
    ('point_before', SIGNAL_ID, _at(5), None, ('ApproachDelay',)),
    ('point_after', SIGNAL_ID, _at(25), None, ()),
    ('range_overlaps_start', SIGNAL_ID, _at(5), _at(12), ('SplitMonitor',)),
    ('range_overlaps_end', SIGNAL_ID, _at(18), _at(28), ()),
    ('range_spans_window', SIGNAL_ID, _at(1), _at(31), ('PurdueCoordination',)),
    ('range_before', SIGNAL_ID, _at(1), _at(5), ('SplitMonitor',)),
    ('range_after', SIGNAL_ID, _at(25), _at(28), ()),
    ('other_signal', OTHER_SIGNAL_ID, None, None, ('ApproachDelay',)),
)

ALL_ON_SIGNAL = frozenset(
    label for label, signal_id, _s, _e, _m in PREDICATE_ROWS if signal_id == SIGNAL_ID
)


def _sqlite_ts(value):
    """Format a datetime the way SQLAlchemy renders one for SQLite."""
    return None if value is None else value.strftime('%Y-%m-%d %H:%M:%S.%f')


def _matching_labels(params):
    """Labels of PREDICATE_ROWS the endpoint's WHERE clause selects for ``params``.

    The endpoint runs against a mock session, so its predicate is never
    executed by a database.  Compile it for SQLite and run it over the fixture
    rows instead: what comes back is the production filter's own verdict.
    """
    mock_session = _session()
    res = _get(mock_session, _reader(), params=params)
    assert res.status_code == status.HTTP_200_OK

    where = str(_comment_stmt(mock_session).whereclause.compile(
        dialect=sqlite_dialect.dialect(), compile_kwargs={'literal_binds': True}
    ))

    con = sqlite3.connect(':memory:')
    try:
        con.execute("ATTACH ':memory:' AS config")
        con.execute(
            'CREATE TABLE config.metric_comment '
            '(id TEXT, signal_id TEXT, anchor_start TEXT, anchor_end TEXT)'
        )
        con.execute(
            'CREATE TABLE config.metric_comment_metric_type '
            '(comment_id TEXT, metric_type_key TEXT)'
        )
        for label, signal_id, anchor_start, anchor_end, metric_types in PREDICATE_ROWS:
            con.execute(
                'INSERT INTO config.metric_comment VALUES (?, ?, ?, ?)',
                (label, signal_id, _sqlite_ts(anchor_start), _sqlite_ts(anchor_end)),
            )
            for key in metric_types:
                con.execute(
                    'INSERT INTO config.metric_comment_metric_type VALUES (?, ?)',
                    (label, key),
                )
        rows = con.execute(f'SELECT id FROM config.metric_comment WHERE {where}').fetchall()
    finally:
        con.close()

    return {row[0] for row in rows}


def _window(**extra):
    params = {'start': WINDOW_START.isoformat(), 'end': WINDOW_END.isoformat()}
    params.update(extra)
    return params


# ---------------------------------------------------------------------------
# Overlay payload
# ---------------------------------------------------------------------------


def test_overlay_returns_the_comments_to_draw_on_the_chart():
    """The overlay hands back each comment with its author snapshot and anchors."""
    comment = _comment_obj(
        anchor_start=_at(12), anchor_end=_at(14), text='Construction here 2023-01'
    )
    res = _get(_session(comments=[comment]), _reader())

    assert res.status_code == status.HTTP_200_OK
    body = res.json()
    assert len(body) == 1
    assert body[0]['id'] == str(comment.id)
    assert body[0]['signal_id'] == SIGNAL_ID
    assert body[0]['text'] == 'Construction here 2023-01'
    assert body[0]['author_uuid'] == str(AUTHOR_ID)
    assert body[0]['author_username'] == 'alice'
    assert body[0]['anchor_start'].startswith('2023-01-12')
    assert body[0]['anchor_end'].startswith('2023-01-14')


def test_overlay_reads_authorship_off_the_row_without_joining_auth_user():
    """Decision 5 - the author snapshot is denormalised; no auth_user join."""
    mock_session = _session(comments=[_comment_obj()])
    assert _get(mock_session, _reader()).status_code == status.HTTP_200_OK

    rendered = str(_comment_stmt(mock_session))
    assert 'auth_user' not in rendered
    assert 'JOIN' not in rendered.upper()


def test_overlay_404_when_the_signal_is_unknown():
    """An unknown parent signal 404s before any comment is fetched."""
    mock_session = _session(signal=None)
    res = _get(mock_session, _reader(), signal_id='SIG-404')

    assert res.status_code == status.HTTP_404_NOT_FOUND
    assert res.json()['detail'] == 'Signal SIG-404 not found'


def test_overlay_is_scoped_to_the_parent_signal():
    """Another signal's comments never appear in this signal's overlay."""
    assert 'other_signal' not in _matching_labels({})
    assert 'other_signal' not in _matching_labels(_window())


# ---------------------------------------------------------------------------
# Read gating - require_access("comments")
# ---------------------------------------------------------------------------


def test_overlay_401_when_the_comments_policy_requires_authentication():
    """The seeded 'comments' policy is authenticated; anonymous reads are refused."""
    policy = [SimpleNamespace(key='access_policy.comments', value='authenticated')]
    res = _get(_session(comments=[_comment_obj()]), None, policy_rows=policy)

    assert res.status_code == status.HTTP_401_UNAUTHORIZED


def test_overlay_200_when_the_comments_policy_is_public():
    """An operator may open the category; then an anonymous chart may read it."""
    policy = [SimpleNamespace(key='access_policy.comments', value='public')]
    res = _get(_session(comments=[_comment_obj()]), None, policy_rows=policy)

    assert res.status_code == status.HTTP_200_OK
    assert len(res.json()) == 1


def test_overlay_is_not_gated_on_the_signal_detail_policy():
    """Decision 2 - 'signal_detail' seeds public and must not stand in for 'comments'."""
    policy = [
        SimpleNamespace(key='access_policy.signal_detail', value='public'),
        SimpleNamespace(key='access_policy.comments', value='authenticated'),
    ]
    res = _get(_session(comments=[_comment_obj()]), None, policy_rows=policy)

    assert res.status_code == status.HTTP_401_UNAUTHORIZED


# ---------------------------------------------------------------------------
# metric_type - repeatable, ANY-of over the m2m
# ---------------------------------------------------------------------------


def test_metric_type_filter_matches_any_requested_type():
    """A comment matches when ANY of its metric types was asked for."""
    matched = _matching_labels({'metric_type': ['ApproachDelay', 'SplitMonitor']})

    # ApproachDelay on one, SplitMonitor on the others - each matches on its own.
    assert 'point_inside' in matched
    assert 'point_before' in matched
    assert 'range_overlaps_start' in matched
    assert 'range_before' in matched
    # Annotated, but with neither requested type.
    assert 'range_spans_window' not in matched


def test_metric_type_filter_excludes_comments_with_no_association():
    """Asking for a metric type excludes comments annotated with none."""
    matched = _matching_labels({'metric_type': ['ApproachDelay']})

    assert 'unanchored' not in matched
    assert 'point_at_start' not in matched
    assert matched == {'point_inside', 'point_before'}


def test_metric_type_filter_does_not_duplicate_a_multiply_annotated_comment():
    """ANY-of goes through a subquery, not a join, so no comment row fans out."""
    mock_session = _session()
    res = _get(mock_session, _reader(), params={'metric_type': ['ApproachDelay', 'SplitMonitor']})
    assert res.status_code == status.HTTP_200_OK

    rendered = str(_comment_stmt(mock_session))
    assert 'JOIN' not in rendered.upper()
    assert 'DISTINCT' not in rendered.upper()
    assert 'metric_comment_metric_type' in rendered


def test_omitting_metric_type_applies_no_metric_type_filter():
    """No metric_type at all -> unannotated comments are returned too."""
    matched = _matching_labels({})

    assert 'unanchored' in matched
    assert matched == ALL_ON_SIGNAL


# ---------------------------------------------------------------------------
# Window - three anchor states, no fourth
# ---------------------------------------------------------------------------


def test_window_always_includes_unanchored_comments():
    """Unanchored notes annotate the chart, not a moment on it - always drawn."""
    assert 'unanchored' in _matching_labels(_window())


def test_window_includes_a_point_anchor_inside_it():
    assert 'point_inside' in _matching_labels(_window())


def test_window_excludes_a_point_anchor_outside_it():
    matched = _matching_labels(_window())

    assert 'point_before' not in matched
    assert 'point_after' not in matched


def test_window_bounds_are_inclusive_for_a_point_anchor():
    matched = _matching_labels(_window())

    assert 'point_at_start' in matched
    assert 'point_at_end' in matched


def test_window_includes_a_range_anchor_that_overlaps_it():
    matched = _matching_labels(_window())

    assert 'range_overlaps_start' in matched
    assert 'range_overlaps_end' in matched
    assert 'range_spans_window' in matched


def test_window_excludes_a_range_anchor_that_does_not_overlap_it():
    matched = _matching_labels(_window())

    assert 'range_before' not in matched
    assert 'range_after' not in matched


def test_window_selects_exactly_the_overlapping_comments():
    """The whole verdict for one window, so no exclusion can hide behind a passing include."""
    assert _matching_labels(_window()) == {
        'unanchored',
        'point_inside',
        'point_at_start',
        'point_at_end',
        'range_overlaps_start',
        'range_overlaps_end',
        'range_spans_window',
    }


def test_a_one_sided_window_bounds_only_the_side_given():
    """Only ``end`` given - nothing later than it, everything earlier stays."""
    matched = _matching_labels({'end': WINDOW_END.isoformat()})

    assert 'point_after' not in matched
    assert 'range_after' not in matched
    assert 'point_before' in matched
    assert 'range_before' in matched
    assert 'unanchored' in matched


def test_omitting_the_window_returns_every_comment_on_the_signal():
    """No window at all - every anchor state comes back."""
    assert _matching_labels({}) == ALL_ON_SIGNAL
