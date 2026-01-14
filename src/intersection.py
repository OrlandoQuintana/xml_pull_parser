from h3 import h3

def expand_to_neighbors(cell_list, k=1):
    """
    Expand base H3 cells to include k-ring neighbors.
    Returns a set.
    """
    expanded = set()
    for cell in cell_list:
        expanded.update(h3.k_ring(cell, k))
    return expanded


def restrict_to_valid(expanded_cells, valid_universe):
    """
    Intersect expanded cells with a larger universe
    """
    return expanded_cells.intersection(valid_universe)


# ---------------------------
# Example usage
# ---------------------------

base_cells = [
    "831c6ffffffffff",
    "831c2fffffffffff",
]

# This is your global universe
all_cells = set([
    "831c6ffffffffff",
    "831c2fffffffffff",
    "831c0fffffffffff",
    "831c5ffffffffff",
])

# 1. Expand base cells to neighbors
expanded = expand_to_neighbors(base_cells, k=1)

# 2. Keep only those neighbors that exist in your universe
final_cells = restrict_to_valid(expanded, all_cells)

print("Base:", len(base_cells))
print("Expanded:", len(expanded))
print("After restriction:", len(final_cells))