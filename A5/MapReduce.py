# CPS610 - Assignment 5, Part 2
# Map-Reduce Implementation for Oscar 2024 Movie Reviews

# DATA STRUCTURES

# Node in a linked list: holds movie info and pointer to next
class Node:
    def __init__(self, movie_info, index):
        self.movie_info = movie_info   
        self.index      = index        
        self.next       = None         # Pointer to next node


# Hash Table: array of buckets, each bucket is a linked-list head
class HashTable:
    def __init__(self, size=20):
        self.size    = size
        self.buckets = [None] * size   # Each slot = head node of linked list
        self.keys    = [None] * size   
        self.counts  = [0]   * size    

    # Simple hash function: sum of ASCII values of key chars mod table size
    def hash_func(self, key):
        return sum(ord(c) for c in key.lower()) % self.size

    # Insert a (movie_name, movie_info) pair into the hash table
    def insert(self, movie_name, movie_info):
        idx      = self.hash_func(movie_name)

        # Count how many nodes already exist for this key (for index field)
        existing = self.counts[idx]
        new_node = Node(movie_info, existing + 1)

        if self.buckets[idx] is None:
            # Empty slot — first occurrence
            self.buckets[idx] = new_node
            self.keys[idx]    = movie_name
        else:
            # Collision handling: walk to end and append
            current = self.buckets[idx]
            while current.next is not None:
                current = current.next
            current.next = new_node

        self.counts[idx] += 1

    # Count nodes in linked list at a given key's bucket
    def count_key(self, movie_name):
        idx  = self.hash_func(movie_name)
        count   = 0
        current = self.buckets[idx]
        while current is not None:
            count  += 1
            current = current.next
        return count

    # Print all entries (for debugging)
    def display(self):
        for i in range(self.size):
            if self.buckets[i] is not None:
                print(f"  [{i}] Key='{self.keys[i]}'  occurrences={self.counts[i]}")


# MOVIE NAMES (the 10 Oscar 2024 nominees - our "keys")
MOVIE_NAMES = [
    "AMERICAN FICTION",
    "ANATOMY OF A FALL",
    "BARBIE",
    "THE HOLDOVERS",
    "KILLERS OF THE FLOWER MOON",
    "MAESTRO",
    "OPPENHEIMER",
    "PAST LIVES",
    "POOR THINGS",
    "THE ZONE OF INTEREST",
]

# Lowercase versions for case-insensitive matching
MOVIE_NAMES_LOWER = [m.lower() for m in MOVIE_NAMES]


# PHASE 1 - SPLIT
# Read a data file and return a list of lines

def split(filename):
    with open(filename, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    print(f"  [SPLIT] '{filename}' → {len(lines)} lines extracted")
    return lines


# PHASE 2 - MAP
# For each line, check if any movie name appears in it.
# If yes, insert (movie_name, line_snippet) into the hash table.
# Returns a HashTable for this data file.

def map_phase(lines, file_label):
    ht = HashTable(size=20)
    for line in lines:
        line_lower = line.lower()
        for movie in MOVIE_NAMES:
            if movie.lower() in line_lower:
                # Use first 60 chars of the line as the "movie_info"
                snippet = line[:60] + ("..." if len(line) > 60 else "")
                ht.insert(movie, snippet)
    print(f"  [MAP]   '{file_label}' hash table populated:")
    ht.display()
    return ht


# PHASE 3 - SHUFFLE
# Merge Hash2, Hash3, Hash4 into Hash1 (the master table)

def shuffle(hash1, other_hashes):
    print("\n  [SHUFFLE] Merging all hash tables into Hash1...")
    for ht in other_hashes:
        for i in range(ht.size):
            if ht.buckets[i] is not None:
                movie_name = ht.keys[i]
                current    = ht.buckets[i]
                while current is not None:
                    hash1.insert(movie_name, current.movie_info)
                    current = current.next
    print("  [SHUFFLE] Merge complete. Master Hash1:")
    hash1.display()
    return hash1


# PHASE 4 - REDUCE
# For each movie key, count total nodes across ALL buckets
# (handles hash collisions where same key might land same slot)
# Returns dict: {movie_name: total_count}

def reduce_phase(master_hash):
    print("\n  [REDUCE] Counting occurrences per movie...")
    result = {}
    for movie in MOVIE_NAMES:
        total = master_hash.count_key(movie)
        if total > 0:
            result[movie] = total
    return result


# QUERY FUNCTION
# Search for a specific movie name across all 4 hash tables
# (simulates querying a single key across distributed machines)

def query_movie(movie_name, hash_tables):
    movie_upper = movie_name.upper()
    if movie_upper not in MOVIE_NAMES:
        print(f"\n  [QUERY] '{movie_name}' is not a recognized Oscar nominee.")
        return

    print(f"\n  [QUERY] Searching for: '{movie_upper}' across all 4 machines...")
    total = 0
    for i, ht in enumerate(hash_tables, 1):
        count = ht.count_key(movie_upper)
        print(f"    Machine {i} (Hash{i}): {count} occurrence(s)")
        total += count
    print(f"  [QUERY] TOTAL occurrences of '{movie_upper}': {total}")
    return total


# MAIN

def main():
    data_files = [
        "aa5-data-file1.txt",
        "aa5-data-file2.txt",
        "aa5-data-file3.txt",
        "aa5-data-file4.txt",
    ]

    print("=" * 60)
    print("  CPS610 A5 - Map-Reduce: Oscar 2024 Movie Mentions")
    print("=" * 60)

    # ---- SPLIT PHASE ----
    print("\n--- PHASE 1: SPLIT ---")
    all_lines = []
    for f in data_files:
        lines = split(f)
        all_lines.append(lines)

    # ---- MAP PHASE ----
    print("\n--- PHASE 2: MAP ---")
    hash_tables = []
    for i, lines in enumerate(all_lines):
        label = data_files[i]
        ht    = map_phase(lines, label)
        hash_tables.append(ht)

    # ---- SHUFFLE PHASE ----
    print("\n--- PHASE 3: SHUFFLE ---")
    master_hash = shuffle(hash_tables[0], hash_tables[1:])

    # ---- REDUCE PHASE ----
    print("\n--- PHASE 4: REDUCE ---")
    final_counts = reduce_phase(master_hash)

    # ---- FINAL OUTPUT ----
    print("\n" + "=" * 60)
    print("  FINAL OUTPUT: (movie_name, total_count)")
    print("=" * 60)
    for movie, count in sorted(final_counts.items(), key=lambda x: -x[1]):
        print(f"  {movie:<30} → {count} mention(s)")

    # ---- QUERY DEMO ----
    # The assignment says: "each run processes ONE input key"
    # Here we demonstrate querying each movie individually
    print("\n" + "=" * 60)
    print("  QUERY MODE: Per-machine breakdown for each movie")
    print("=" * 60)
    for movie in MOVIE_NAMES:
        query_movie(movie, hash_tables)

    # ---- BONUS: Serialized Transactions ----
    print("\n" + "=" * 60)
    print("  BONUS: Concurrent / Serialized Transaction Analysis")
    print("=" * 60)
    print("""
  Parts that CAN run concurrently (no conflicts):
  ------------------------------------------------
  1. SPLIT phase: all 4 files can be split simultaneously
     → Transaction T1: split(file1)
     → Transaction T2: split(file2)
     → Transaction T3: split(file3)
     → Transaction T4: split(file4)

  2. MAP phase: all 4 hash tables can be built simultaneously
     → Transaction T5: map(file1_lines) → Hash1
     → Transaction T6: map(file2_lines) → Hash2
     → Transaction T7: map(file3_lines) → Hash3
     → Transaction T8: map(file4_lines) → Hash4

  3. SHUFFLE phase: must be sequential (writes to shared Hash1)
     → Transaction T9: merge Hash2→Hash1
     → Transaction T10: merge Hash3→Hash1
     → Transaction T11: merge Hash4→Hash1

  4. REDUCE phase: each movie count is independent
     → T12..T21: count each of the 10 movies concurrently

  Total serialized transactions N = 21
  (T1-T4 parallel, T5-T8 parallel, T9-T11 sequential, T12-T21 parallel)
    """)


if __name__ == "__main__":
    main()