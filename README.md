# tap-gem

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Gem API](https://api.gem.com/v0/reference)
- Extracts the following resources:
  - Candidates
  - Events
  - Projects
  - Users
  - Project Candidates
- Outputs the schema for each resource
- Incrementally pulls data based on the input state
