"""Random content for the simulator.

Author selection is two-tier:
  1. A curated weighted pool — 5 "core contributors" who dominate the
     leaderboard, plus a handful of regulars and occasionals.
  2. A drive-by pool — randomly named one-offs who appear 1–5 times,
     then never publish again. Each time a drive-by is picked we either
     reuse a still-active one or invent a brand new name.

Article bodies are generated from a bundled English word list
(wonderwords) so practically every publication is unique."""
from __future__ import annotations

import random

from wonderwords import RandomSentence, RandomWord

_word = RandomWord()
_sentence = RandomSentence()

# (author, weight). Curated pool — these names persist across the run.
AUTHORS_WEIGHTED: list[tuple[str, int]] = [
    # Core contributors
    ("Ada Lovelace", 9),
    ("Boris Hartmann", 8),
    ("Camille Rivera", 7),
    ("Darya Volkova", 7),
    ("Elias Okafor", 6),
    # Regulars
    ("Fumiko Sato", 3),
    ("Gunnar Holm", 3),
    ("Hana Park", 3),
    ("Idris Khan", 3),
    # Occasional
    ("Juno Marchetti", 1),
    ("Kofi Adusei", 1),
    ("Lena Forsberg", 1),
    ("Marius Brandt", 1),
    ("Nadia Solis", 1),
]
_AUTHORS, _WEIGHTS = zip(*AUTHORS_WEIGHTED)

# Drive-by name parts. Random first + random last on demand.
_FIRST_NAMES = [
    "Alex", "Sam", "Jordan", "Riley", "Casey", "Morgan", "Jamie", "Quinn",
    "Avery", "Reese", "Sage", "Rowan", "Dakota", "Skyler", "Emerson",
    "Indra", "Mateo", "Zara", "Niamh", "Tomas", "Luka", "Yara", "Mira",
    "Ezra", "Aki", "Soren", "Lior", "Anya", "Cyrus", "Tova", "Bjorn",
]
_LAST_NAMES = [
    "Tanaka", "Chen", "Kowalski", "Hassan", "Andersson", "Mbeki", "Rao",
    "Esposito", "Larsen", "Petrov", "Olawale", "Singh", "Nakamura",
    "Dubois", "Romano", "Yamada", "Schwartz", "Costa", "Murakami",
    "Ahmed", "Bauer", "Lindqvist", "Vargas", "Brennan", "Holt", "Ó Briain",
]

# Drive-bys with remaining post budget. Pruned on each pick.
_drive_bys: list[list] = []  # [name, remaining_posts]
DRIVE_BY_PROBABILITY = 0.15  # ~15% of articles come from a drive-by


def _new_drive_by_name() -> str:
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def random_author() -> str:
    if random.random() < DRIVE_BY_PROBABILITY:
        # Drop anyone whose budget is exhausted.
        _drive_bys[:] = [d for d in _drive_bys if d[1] > 0]
        # 60% chance to reuse an existing drive-by (if any are still alive),
        # 40% chance to invent a fresh new name.
        if _drive_bys and random.random() < 0.6:
            pick = random.choice(_drive_bys)
            pick[1] -= 1
            return pick[0]
        name = _new_drive_by_name()
        # New drive-by publishes 1..5 total times (we just used one).
        remaining = random.randint(0, 4)
        _drive_bys.append([name, remaining])
        return name
    return random.choices(_AUTHORS, weights=_WEIGHTS, k=1)[0]


def _safe_word(parts: list[str] | None = None) -> str:
    """wonderwords can occasionally raise NoWordsToChooseFrom if filters
    yield an empty set; fall back to an unfiltered word."""
    try:
        if parts:
            return _word.word(include_parts_of_speech=parts)
        return _word.word()
    except Exception:  # noqa: BLE001
        return _word.word()


def _random_title() -> str:
    n = random.randint(2, 5)
    words = [_safe_word(["nouns", "adjectives"]) for _ in range(n)]
    return " ".join(w.capitalize() for w in words)


def random_poem() -> tuple[str, str]:
    title = _random_title()
    n_lines = random.randint(3, 8)
    lines = []
    for _ in range(n_lines):
        n_words = random.randint(4, 9)
        lines.append(" ".join(_safe_word() for _ in range(n_words)))
    return title, "\n".join(lines)


def random_article() -> tuple[str, str]:
    title = _random_title()
    n_sentences = random.randint(5, 30)
    sentences = []
    for _ in range(n_sentences):
        try:
            sentences.append(_sentence.sentence())
        except Exception:  # noqa: BLE001
            n = random.randint(6, 14)
            sentences.append(" ".join(_safe_word() for _ in range(n)).capitalize() + ".")
    return title, " ".join(sentences)


def random_publication() -> tuple[str, str, str, str]:
    """Returns (author, title, body, kind)."""
    if random.random() < 0.35:
        title, body = random_poem()
        kind = "poem"
    else:
        title, body = random_article()
        kind = "article"
    return random_author(), title, body, kind
