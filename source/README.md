# Adapter

# Purpose

This folder contains implementations for external data sources.

The different data sources are under different folders because:

1. In most cases, only a few adapters are needed for a program. Separating
them would reduce the total compiled binary size.
2. Easier to implement new adapters.
