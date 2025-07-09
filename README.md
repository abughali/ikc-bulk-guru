# IBM Knowledge Catalog (IKC) Bulk Guru

This repo demonstrates how the IKC API (formerly Watson Data Platform API) can be used to perform quick updates in bulk for some laborious tasks when done through the UI.

## Prerequisites and Recommended Tools

- Cloud Pak for Data `5.0` or above
- Github Desktop <https://desktop.github.com/>
- Git (optional for integration with VSC) <https://git-scm.com/downloads>
- Python `3.8` or above <https://www.python.org/downloads/>
- Visual Studio Code (VSC) <https://code.visualstudio.com/>
- Python Extension of VSC <https://code.visualstudio.com/docs/python/python-tutorial>

## Installation

1. Make sure the prerequisites are installed.
2. Clone this github repository.
3. Install necessary dependencies:

```sh
pip install -r requirements.txt
```

4. Create `.env` file with Secrets:

```sh
cp .envExample .env
```

5. Configure your `.env` file with your CPD environment details:

### Environment Configuration Options

The `.env` file supports multiple authentication methods and deployment types. Choose the configuration that matches your CPD environment:

#### For Cloud Pak for Data Software (On-Premises) with Password Authentication
```env
CPD_HOST=your-cpd-cluster.com
ENV_TYPE=SW
AUTH_TYPE=PASSWORD
USERNAME=your-username
PASSWORD=your-password
CATALOG_ID=your-catalog-id
PROJECT_ID=your-project-id
```

#### For Cloud Pak for Data Software (On-Premises) with API Key Authentication
```env
CPD_HOST=your-cpd-cluster.com
ENV_TYPE=SW
AUTH_TYPE=API_KEY
USERNAME=your-username
API_KEY=your-api-key
CATALOG_ID=your-catalog-id
PROJECT_ID=your-project-id
```

#### For IBM Cloud Pak for Data as a Service (SaaS)
```env
CPD_HOST=your-cpd-saas-url.com
ENV_TYPE=SAAS
API_KEY=your-ibm-cloud-api-key
CATALOG_ID=your-catalog-id
PROJECT_ID=your-project-id
```

#### Environment Variables Reference

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `CPD_HOST` | Yes | CPD cluster hostname (without https://) | `cpd-cluster.example.com` |
| `CPD_FS_HOST` | Conditional | CPD flight service endpoint (without protocol and port) | `flight-service.cpd-cluster.example.com` |
| `ENV_TYPE` | No | Environment type: `SW` (software) or `SAAS` | `SW` (default) |
| `AUTH_TYPE` | No | Authentication method: `PASSWORD` or `API_KEY` | `PASSWORD` (default) |
| `USERNAME` | Conditional | CPD username (required for SW environments) | `admin` |
| `PASSWORD` | Conditional | CPD password (required when AUTH_TYPE=PASSWORD) | `your-secure-password` |
| `API_KEY` | Conditional | API key (required for SAAS or when AUTH_TYPE=API_KEY) | `your-api-key` |
| `CATALOG_ID` | Optional | Target catalog ID for asset operations | `12345678-1234-1234-1234-123456789abc` |
| `PROJECT_ID` | Optional | Project ID for project-scoped operations | `87654321-4321-4321-4321-cba987654321` |

#### How to Find Your Configuration Values

**CPD_HOST:**
- For on-premises: Your CPD cluster URL without `https://`
- For SaaS: Found in your IBM Cloud console

**CATALOG_ID:**
1. Log into your CPD web interface
2. Navigate to **Data catalogs**
3. Open your target catalog
4. Find the catalog ID in the URL: `/data/catalogs/{CATALOG_ID}/`

**API_KEY (for SaaS environments):**
1. Go to [IBM Cloud API Keys](https://cloud.ibm.com/iam/apikeys)
2. Create a new API key or use an existing one
3. Ensure the key has appropriate permissions for your CPD instance

**API_KEY (for on-premises with API key auth):**
1. Log into CPD web interface
2. Go to **Profile** → **API keys**
3. Generate a new API key

Reference: https://www.ibm.com/docs/en/cloud-paks/cp-data/5.1.x?topic=tutorials-generating-api-keys#api-keys__platform

**PROJECT_ID (optional):**
1. Navigate to **Projects** in CPD
2. Open your target project
3. Find the project ID in the URL: `/data/projects/{PROJECT_ID}/`

#### Authentication Method Recommendations

- **Password Authentication**: Simplest for development and testing
- **API Key Authentication**: Recommended for production use and automation
- **SaaS**: Always requires IBM Cloud API key authentication

#### Security Notes

- Never commit your `.env` file to version control
- Use API keys instead of passwords for production environments
- Rotate API keys regularly according to your security policies
- Ensure your CPD user has appropriate permissions for catalog and asset operations

### Swagger UIs and other API docs

- <https://{cp4d_hostname}/v3/search/api/explorer>
- <https://cloud.ibm.com/apidocs/watson-data-api>

### Programming resources

- Beginners Guide to Python: <https://wiki.python.org/moin/BeginnersGuide/NonProgrammers>
- Programmers Guide to Python: <https://wiki.python.org/moin/BeginnersGuide/Programmers>
- Getting Started with Pandas: <https://pandas.pydata.org/docs/getting_started/index.html>

## Use Cases

Following are the key use cases that are covered in this repo:

### How do I search for artifacts?

Using Global Search to demonstrate how to find and export artifacts in IKC.

#### Export Artifacts to CSV

Use `export_artifacts.py` to export all governance artifacts from your CPD environment to CSV files:

```sh
python export_artifacts.py
```

This will create timestamped CSV files in the `exports/` directory:
- `glossary_term_YYYYMMDD_HHMMSS.csv`
- `data_class_YYYYMMDD_HHMMSS.csv`
- `classification_YYYYMMDD_HHMMSS.csv`

**CSV Output Columns:**
- `artifact_id` - Unique identifier for the artifact
- `categories.primary_category_name` - Primary category assignment
- `entity.artifacts.global_id` - Global identifier
- `metadata.name` - Artifact name

### How do I perform mass (bulk) updates?

#### Bulk Assignment of Terms, Classifications, and Data Classes

Use the main bulk assignment script to assign governance artifacts to asset columns in bulk.

#### Prepare a CSV

Create a CSV file with the following format (with header row):

```csv
Asset Name,Column Name,Column Description,Term Name,Term Category,Classification Name,Classification Category,Data Class Name,Data Class Category,Tags
```

**Example CSV content:**
```csv
Asset Name,Column Name,Column Description,Term Name,Term Category,Classification Name,Classification Category,Data Class Name,Data Class Category,Tags
ADMIN.T_US_STATES,ABBREV,Abbreviation of name,Country Code,Location,Confidential,[uncategorized],Country Code,Location Data Classes,TAG1
ADMIN.T_US_STATES,CODE,Code of the country name,Country Code,Location,Confidential,[uncategorized],US State Code,[uncategorized],TAG1
ADMIN.T_US_STATES,STATE,The state name,Country Code,Location,Confidential,[uncategorized],US State Code,[uncategorized],TAG1|TAG2
```

**Column Descriptions:**
- **Asset Name**: Exact name of the data asset in your catalog or project
- **Column Name**: Exact name of the column within the asset
- **Column Description**: Description of the column
- **Term Name**: Business term to assign (leave empty to skip)
- **Term Category**: Category where the term is located
- **Classification Name**: Classification to assign (leave empty to skip)
- **Classification Category**: Category where the classification is located
- **Data Class Name**: Data class to assign (leave empty to skip)
- **Data Class Category**: Category where the data class is located
- **Column Tags**: Tags pipe `|` separated (leave empty to skip)


#### Run the Bulk Assignment

```sh
python bulk_assign_project.py
# OR
python bulk_assign_catalog.py
```

By default, it looks for `col_term_map.csv` in the current directory. You can modify the script to use a different input file.

**The script will:**
1. **Preload all artifacts** into memory for fast lookups
2. **Process each row** in your CSV file
3. **Validate** that assets and columns exist
4. **Assign** terms, classifications, and data classes as specified
5. **Generate** a detailed results CSV with success/failure status

#### Understanding the Output

The script creates an output file `out/{input_filename}_{date}_{time}.csv` with additional columns:
- **Term Result**: SUCCESS, ERROR, or SKIPPED
- **Classification Result**: SUCCESS, ERROR, or SKIPPED  
- **Data Class Result**: SUCCESS, ERROR, or SKIPPED
- **Update Status**: SUCCESS, ERROR, or SKIPPED

**Example output:**
```csv
Asset Name,Column Name,Column Description,Term Name,Term Category,Classification Name,Classification Category,Data Class Name,Data Class Category,Tags,Term Result,Classification Result,Data Class Result,Update Status
T_US_STATES,ABBREV,Abbreviation of name,Country Code,Location,Confidential,[uncategorized],Country Code,Location Data Classes,TAG1,SUCCESS,SUCCESS,SUCCESS,SUCCESS
T_US_STATES,CODE,Code of the country name,Country Code,Location,Confidential,[uncategorized],US State Code,[uncategorized],TAG1,SUCCESS,SUCCESS,SUCCESS,SUCCESS
T_US_STATES,STATE,The state name,Country Code,Location,Confidential,[uncategorized],US State Code,[uncategorized],TAG1|TAG2,SUCCESS,SUCCESS,SUCCESS,SUCCESS
```

## Scripts Overview

### `export_artifacts.py`
Exports all governance artifacts (terms, classifications, data classes) to CSV files for analysis or backup.

**Features:**
- Handles pagination for large datasets
- Exports to timestamped CSV files

### `bulks_assign_project.py` and `bulks_assign_catalog.py` (Bulk Assignment)
Performs bulk assignment of governance artifacts to asset columns.

**Features:**
- **Smart caching**: Preloads all artifacts to minimize API calls
- **Validation**: Checks asset and column existence before updates
- **Granular tracking**: Reports success/failure for each assignment type
- **Incremental updates**: Only updates specified fields, preserves existing metadata
- **Detailed logging**: Console output shows progress and issues
- **Results tracking**: Generates comprehensive output CSV

## Best Practices

### Data Preparation
- **Exact naming**: Asset and column names must match exactly (case-sensitive)
- **Validate categories**: Ensure term/classification/data class categories exist
- **Test with small batches**: Start with 10-20 rows to validate your CSV format

### Error Handling
- **Check output CSV**: Always review the results file for any failures
- **Common errors**:
  - Asset not found: Check asset name spelling and catalog access
  - Column not found: Verify column exists in asset schema
  - Artifact not found: Confirm term/classification/data class names and categories

## Troubleshooting

### Authentication Issues
```
Error: 401 Unauthorized
```
- Verify CPD credentials in `.env` file
- Check if API key is valid and not expired
- Ensure user has necessary permissions

### Asset Not Found
```
Asset 'asset_name' is either not found or duplicated
```
- Verify asset name matches exactly (case-sensitive)
- Check catalog_id is correct
- Ensure asset is in PUBLISHED state

### Column Not Found
```
Column 'column_name' not found in asset
```
- Verify column name matches schema exactly
- Check if asset has been profiled/discovered
- Ensure column_info exists in asset metadata

### Artifact Lookup Failures
```
Term 'term_name' with category 'category' not found
```
- Verify artifact names and categories are exact matches
- Check if artifacts are in PUBLISHED state
- Run `export_artifacts.py` to see available artifacts
