# MrPlant FEED -> Jetshop Integration

Production-minded synchronization service for MrPlant. FEED is the source of truth and Jetshop is updated via SOAP. The sync is mapping-driven, logged, and supports dry-run.

## Requirements
- Python 3.10+
- `pip install -r requirements.txt`

Optional for tests:
- `pip install -r requirements-dev.txt`

## Configuration
Create a `.env` in the project root (values from the spec):

```
FEED_TOKEN_URL=https://mrplant-feed.isysnet.no/token-server/oauth/token
FEED_CLIENT_ID=feedMrPlant
FEED_CLIENT_SECRET=cs7U7Gk6LCMFQuZjD2YzJ5WV93XdRgpK
FEED_EXPORT_URL=https://mrplant-feed.isysnet.no/export/export/full

JETSHOP_SOAP_URL=https://integration.jetshop.se/Webservice20/v3.0/webservice.asmx
JETSHOP_USERNAME=<basic-auth-username>
JETSHOP_PASSWORD=<basic-auth-password>
JETSHOP_SHOP_ID=<shop-id-to-send-in-soap-header>
JETSHOP_TEMPLATE_ID=1

CULTURES=sv-SE,nb-NO
LOG_FILE=logs/integration.log
MAPPING_FILE=mappings/mapping.yaml

# Optional: override SOAP header XML (string). If omitted, <ShopId> is used.
JETSHOP_SOAP_HEADER_XML=
```

## Mapping
Mapping lives at `mappings/mapping.yaml` and controls all field synchronization. Only allowlisted dynamic fields are synced. Type enforcement and coercion policy are configurable per field.

## Usage
Validate mapping:
```
python -m src.main validate-mapping
```

Discover unmapped fields:
```
python -m src.main discover-mapping --productNo 1092-10 --since 2025-01-01T00:00:00Z
```
This writes `mappings/mapping_suggestions.yaml`.

Sync (normal run):
```
python -m src.main sync --since 2025-01-01T00:00:00Z
```

Sync (dry-run):
```
python -m src.main sync --since 2025-01-01T00:00:00Z --dry-run
```

Optional args:
- `--productNo` to sync a single product.
- `--limit N` to cap processed products.
- `--mapping PATH` to use a custom mapping file.

## Outputs
- Logs: console + rotating file (`logs/integration.log`).
- Dry-run diffs: `diffs/<productNo>.json`.
- State: `state/last_run.json` (updated after successful runs).

## Scheduling
Use Windows Task Scheduler or cron to run:
```
python -m src.main sync --since <ISO>
```

## Notes
- FEED is the source of truth.
- Images are synced from FEED media (base64 download + UploadImage + Product_AddUpdateImages).
- Prices are synced via Jetshop price lists (`PriceList_UpdateArticleIncVAT`) using `price_lists` in `mappings/mapping.yaml`. Product price is not sent in `Product_AddUpdate`.
