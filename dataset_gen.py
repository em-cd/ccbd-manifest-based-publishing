import pyarrow as pa
import pyarrow.parquet as pq
import random
import os
import argparse
from datetime import datetime, timedelta

SEED = 1813  # Year Pride and Prejudice was published

# ═══════════════════════════════════════════════════════
# 📜 Pride and Prejudice: The Social Season Dataset
# ═══════════════════════════════════════════════════════

CHARACTERS = [
    "Mr. Darcy", "Elizabeth Bennet", "Mr. Bingley", "Jane Bennet",
    "Lydia Bennet", "Mr. Wickham", "Mrs. Bennet", "Mr. Bennet",
    "Mr. Collins", "Charlotte Lucas", "Lady Catherine", "Georgiana Darcy",
    "Caroline Bingley", "Kitty Bennet", "Mary Bennet", "Colonel Fitzwilliam"
]

LOCATIONS = [
    "Longbourn", "Netherfield", "Pemberley", "Meryton",
    "Rosings Park", "London", "Brighton", "Lambton"
]

EVENT_TYPES = [
    "ball_attendance", "letter_written", "proposal_rejected", "proposal_accepted",
    "gossip_spread", "piano_played_badly", "long_walk_taken", "witty_remark_delivered",
    "pride_displayed", "prejudice_displayed", "awkward_silence",
    "scandalous_elopement", "fortune_discussed", "rain_encounter",
    "judgemental_glance", "dramatic_hand_flex"
]

TOPICS = [
    "five thousand a year", "the entail", "officers in Meryton",
    "accomplishments of a lady", "tolerable but not handsome enough",
    "most ardently", "improper behavior at the ball",
    "connections in trade", "her fine eyes", "the lack of propriety",
    "she is barely tolerable", "what excellent boiled potatoes",
    "think only of the past as its remembrance gives you pleasure",
    "a single man in possession of a good fortune",
    "you have bewitched me body and soul",
    "my good opinion once lost is lost forever"
]

MOODS = [
    "vexed", "amused", "mortified", "delighted",
    "indifferent", "furious", "swooning", "scandalized",
    "smug", "melancholy", "bewildered", "impertinent"
]

# ═══════════════════════════════════════════════════════
# 📝 Diary entry building blocks
# ═══════════════════════════════════════════════════════

STARTERS = [
    "I must confess that", "It is a truth universally acknowledged that",
    "I am most grievously vexed for", "To my utter astonishment",
    "Mama insists that", "I have been reflecting upon the fact that",
    "It cannot be denied that", "I write with great urgency because",
    "One can hardly believe that", "I am mortified to report that",
    "Heaven forgive me but", "Despite my better judgement",
    "I dare not tell Jane but", "Papa merely laughed when",
    "Much to Lady Catherine's horror", "I shall never forgive myself for",
]

MIDDLES = [
    "Mr. Darcy stared at me most intensely",
    "Mr. Collins spoke for forty-five minutes without pause",
    "Lydia shrieked about officers at breakfast again",
    "Mr. Bingley smiled at Jane in that hopeless way",
    "Caroline Bingley made a cutting remark about my petticoat",
    "Wickham charmed every lady in the room effortlessly",
    "Mrs. Bennet had her nerves attacked most violently",
    "Mary played the pianoforte until we all suffered greatly",
    "Lady Catherine questioned my accomplishments with great disdain",
    "Mr. Bennet retreated to his library without a word",
    "I walked three miles through the mud with great determination",
    "Charlotte accepted Mr. Collins which I cannot comprehend",
    "a gentleman of considerable fortune arrived in the neighbourhood",
    "the entire assembly whispered about ten thousand a year",
    "I caught Mr. Darcy flexing his hand most curiously",
    "Kitty coughed dramatically to gain attention at dinner",
]

ENDINGS = [
    "and I have never been more embarrassed in my life.",
    "which I find both improper and utterly delightful.",
    "and I blame the entail for everything.",
    "but I shall think on it no further. Probably.",
    "and Mama nearly fainted from the excitement of it all.",
    "yet I remain composed, as a lady ought to be.",
    "and I suspect the rain was somehow to blame.",
    "which confirms my opinion that all men are ridiculous.",
    "and I intend to take a very long walk about it.",
    "but five thousand a year can excuse a great deal.",
    "and I shall record no more for my nerves cannot bear it.",
    "though I would rather die than admit it aloud.",
    "and the whole of Meryton shall hear of it by morning.",
    "which only proves that first impressions are most unreliable.",
    "and I am determined to laugh at it rather than cry.",
    "but I confess my heart betrayed my good sense entirely.",
]

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# ═══════════════════════════════════════════════════════
# Dataset sizes
# ═══════════════════════════════════════════════════════

SIZES = {
    "S": 5_000_000,
    "M": 25_000_000,
    "L": 100_000_000,
}

ROWS_PER_FILE = 1_000_000


# ═══════════════════════════════════════════════════════
# 📖 Diary entry generator
# ═══════════════════════════════════════════════════════

def generate_diary_entry(rng):
    """Generate a unique Pride and Prejudice diary entry."""
    starter = rng.choice(STARTERS)
    middle = rng.choice(MIDDLES)
    ending = rng.choice(ENDINGS)

    day = rng.randint(1, 28)
    month = rng.choice(MONTHS)
    hour = rng.randint(1, 12)
    minute = rng.randint(0, 59)
    ref = rng.randint(0, 999999)
    mood = rng.choice(MOODS)
    location = rng.choice(LOCATIONS)
    character = rng.choice(CHARACTERS)

    # Random padding to prevent Parquet from compressing too aggressively
    noise = ''.join(rng.choices('abcdefghijklmnopqrstuvwxyz ', k=200))

    return (
        f"[{day} {month}, quarter past {hour}:{minute:02d}] "
        f"{starter} {middle}, {ending} "
        f"Witnessed by {character} near {location}. "
        f"Mood of the room: {mood}. "
        f"(Diary ref: {ref:06d}) "
        f"Postscript: {noise}"
    )


# ═══════════════════════════════════════════════════════
# 🎲 Batch generator
# ═══════════════════════════════════════════════════════

def generate_value(event, rng):
    """Generate a context-appropriate value for each event type."""
    if event == "fortune_discussed":
        return round(rng.choice([5000, 10000, 2000, 100, 50]), 2)
    elif event == "ball_attendance":
        return round(rng.uniform(1, 10), 1)
    elif event == "letter_written":
        return round(rng.uniform(1, 20), 0)
    elif event in ["proposal_rejected", "proposal_accepted"]:
        return round(rng.uniform(0, 100), 0)
    elif event == "gossip_spread":
        return round(rng.uniform(1, 50), 0)
    elif event == "piano_played_badly":
        return round(rng.uniform(1, 10), 1)
    elif event == "long_walk_taken":
        return round(rng.uniform(1, 6), 1)
    elif event == "dramatic_hand_flex":
        return round(rng.uniform(80, 100), 1)
    elif event == "rain_encounter":
        return round(rng.uniform(1, 10), 1)
    elif event in ["pride_displayed", "prejudice_displayed"]:
        return round(rng.uniform(50, 100), 1)
    else:
        return round(rng.uniform(0, 100), 2)


def generate_batch(num_rows, rng):
    """Generate a batch of rows as a PyArrow table."""
    base_time = datetime(1812, 1, 1)

    ts = []
    character_ids = []
    characters = []
    locations = []
    event_types = []
    topics = []
    moods = []
    values = []
    diary = []

    for _ in range(num_rows):
        event = rng.choice(EVENT_TYPES)
        character = rng.choice(CHARACTERS)

        ts.append(base_time + timedelta(seconds=rng.randint(0, 365 * 24 * 3600)))
        character_ids.append(CHARACTERS.index(character) + 1)
        characters.append(character)
        locations.append(rng.choice(LOCATIONS))
        event_types.append(event)
        topics.append(rng.choice(TOPICS))
        moods.append(rng.choice(MOODS))
        values.append(generate_value(event, rng))
        diary.append(generate_diary_entry(rng))

    return pa.table({
        "ts": ts,
        "user_id": character_ids,
        "character": characters,
        "region": locations,
        "event_type": event_types,
        "topic": topics,
        "mood": moods,
        "value": values,
        "payload": diary,
    })


# ═══════════════════════════════════════════════════════
# 💾 Dataset writer
# ═══════════════════════════════════════════════════════

def generate_dataset(size_label, output_dir="data"):
    total_rows = SIZES[size_label]
    out_path = os.path.join(output_dir, size_label)
    os.makedirs(out_path, exist_ok=True)

    rng = random.Random(SEED)
    rows_written = 0
    file_num = 0

    print(f"\n📜 Generating {size_label} dataset ({total_rows:,} rows)...")
    print(f"   Output: {out_path}/\n")

    while rows_written < total_rows:
        batch_size = min(ROWS_PER_FILE, total_rows - rows_written)
        table = generate_batch(batch_size, rng)

        file_path = os.path.join(out_path, f"part-{file_num:04d}.parquet")
        pq.write_table(table, file_path)

        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        rows_written += batch_size
        file_num += 1

        pct = (rows_written / total_rows) * 100
        print(f"  ✅ part-{file_num - 1:04d}.parquet | {batch_size:>10,} rows | {file_size_mb:>8.1f} MB | {pct:5.1f}%")

    total_size = sum(
        os.path.getsize(os.path.join(out_path, f))
        for f in os.listdir(out_path)
    )
    print(f"\n🎩 {size_label} complete!")
    print(f"   Files: {file_num}")
    print(f"   Rows:  {total_rows:,}")
    print(f"   Size:  {total_size / 1e9:.2f} GB")

    return total_size


def run_test(output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "test")
    os.makedirs(out_path, exist_ok=True)

    """Generate a tiny test dataset and print fun stats."""
    rng = random.Random(SEED)
    table = generate_batch(1000, rng)

    file_path = os.path.join(out_path, "test_output.parquet")
    pq.write_table(table, file_path)

    df = table.to_pandas()
    size_mb = os.path.getsize(file_path) / (1024 * 1024)

    print("📜✨ Pride and Prejudice: The Social Season Dataset ✨📜")
    print(f"Generated {table.num_rows} events ({size_mb:.2f} MB)\n")
    print(f"Schema:\n{table.schema}\n")
    print("Sample events:")
    print(df[["user_id", "ts", "character", "region", "event_type", "value"]].head(10).to_string())

    print(f"\n🎩 Season Highlights:")
    print(f"  Balls attended: {len(df[df['event_type'] == 'ball_attendance'])}")
    print(f"  Letters written: {int(df[df['event_type'] == 'letter_written']['value'].sum())} pages")
    print(f"  Proposals rejected: {len(df[df['event_type'] == 'proposal_rejected'])} 💔")
    print(f"  Proposals accepted: {len(df[df['event_type'] == 'proposal_accepted'])} 💍")
    print(f"  Gossip incidents: {len(df[df['event_type'] == 'gossip_spread'])}")
    print(f"  Dramatic hand flexes: {len(df[df['event_type'] == 'dramatic_hand_flex'])} 🫠")
    print(f"  Rain encounters: {len(df[df['event_type'] == 'rain_encounter'])} 🌧️")
    print(f"  Awkward silences: {len(df[df['event_type'] == 'awkward_silence'])}")
    print(f"  Witty remarks: {len(df[df['event_type'] == 'witty_remark_delivered'])}")
    print(f"  Miles walked: {df[df['event_type'] == 'long_walk_taken']['value'].sum():.1f} 👗")
    print(f"  Piano cringe moments: {len(df[df['event_type'] == 'piano_played_badly'])} (sorry Mary)")

    print(f"\n📊 Most Active Characters:")
    print(df['character'].value_counts().head(5).to_string())

    print(f"\n📍 Most Popular Location: {df['region'].mode()[0]}")
    print(f"😤 Most Common Mood: {df['mood'].mode()[0]}")
    print(f'\n💬 Most Discussed: "{df["topic"].mode()[0]}"')

    print(f"\n📝 Sample Diary Entry:")
    print(f"  {df['payload'].iloc[0][:200]}...")


# ═══════════════════════════════════════════════════════
# 🚀 Main
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="📜 Pride and Prejudice: Social Season Dataset Generator"
    )
    parser.add_argument(
        "--size",
        choices=["S", "M", "L", "test"],
        default="test",
        help="Dataset size: test (1K rows), S (~1GB), M (~5GB), L (~15GB)"
    )
    parser.add_argument(
        "--output",
        default="data",
        help="Output directory (default: data/)"
    )
    args = parser.parse_args()

    if args.size == "test":
        run_test()
    else:
        generate_dataset(args.size, args.output)