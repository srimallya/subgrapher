# Subgrapher Local Embedding Runtime Assets

This directory is reserved for bundled local embedding model assets used by Path-C similarity.

Current runtime uses the built-in deterministic hash-embedding fallback (`hybrid:local-hash-embedding-v1`) and does not require external downloads.

If a larger local model is added later, place model binaries/configuration in this directory and update `runtime/pathc_similarity.js` accordingly.
