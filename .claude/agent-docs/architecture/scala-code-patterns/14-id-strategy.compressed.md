<!-- GENERATED FILE — DO NOT EDIT. Source: docs/architecture/scala-code-patterns/14-id-strategy.md -->

# ID Strategy

**Pattern**: Natural keys for legislative data (Congress.gov provides stable identifiers). Generated UUIDs for RepCheck-specific entities.

## Legislative IDs (Natural Keys)

| Entity | ID Format | Source | Example |
|---|---|---|---|
| Bill | `{type}-{number}-{congress}` | Composed from API fields | `hr-1234-118` |
| Member | `{bioguideId}` | Congress.gov bioguide ID | `A000360` |
| Vote | `{chamber}-{rollNumber}-{congress}-{session}` | Composed from API fields | `house-123-118-1` |
| Amendment | `{type}-{number}-{congress}` | Congress.gov amendment ID | `hamdt-456-118` |

## RepCheck-Specific IDs (Generated)

| Entity | ID Format | Example |
|---|---|---|
| User | UUID v4 | `550e8400-e29b-41d4-a716-446655440000` |
| Analysis | `{billId}-{provider}-{timestamp}` | `hr-1234-118-claude-20250319T120000Z` |
| Score | `{userId}-{memberId}` | Composite natural key |
| Pipeline Run | UUID v4 | `7c9e6679-7425-40de-944b-e07fc1f90ae7` |
| Processing Result | `{runId}-{entityId}` | Composite natural key |

## Rules

- **Never** generate UUIDs for legislative data — always use Congress.gov's natural identifiers
- Bill IDs are composed as `{type}-{number}-{congress}` — lowercase type
- Analysis IDs include the provider name to support multi-LLM analysis of the same bill
- AlloyDB rows use natural composite keys as the PRIMARY KEY — no surrogate IDs for legislative data