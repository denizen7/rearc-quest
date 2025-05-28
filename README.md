# Rearc Data Quest - Brandon Young

This project is a full-featured data pipeline built using AWS CDK, Lambda, S3, SQS, and Python. It automates the ingestion, processing, and analysis of data from public sources such as the Bureau of Labor Statistics (BLS) and DataUSA.

The repo contains all infrastructure-as-code, Lambda scripts, and a Jupyter notebook for analytics. It's designed for deployment on AWS using the AWS CDK (Cloud Development Kit).

---

## 📁 Project Structure

```
rearc-quest/
│
├── lambda/                          # All Lambda functions
│   ├── bls_file_sync/              # Lambda for syncing BLS data
│   │   ├── bls_file_sync.py
│   │   └── requirements.txt
│   ├── fetch_datausa_population/   # Lambda for fetching DataUSA API data
│   │   ├── fetch_datausa_population.py
│   │   └── requirements.txt
│   └── report_generator/           # Lambda for analytics/reporting
│       ├── report_generator.py
│       └── requirements.txt
│
├── analysis.ipynb                  # Jupyter notebook for analysis (PySpark & Pandas)
├── app.py                          # AWS CDK app definition
├── cdk.json                        # CDK config
├── requirements.txt                # CDK and Python dependencies
├── .gitignore
└── venv/                           # Virtual environment (excluded in `.gitignore`)
```

---

## 🚀 What It Does

### Part 1: Sync BLS Data
Scrapes the [BLS PR time series directory](https://download.bls.gov/pub/time.series/pr/) and uploads new or updated files to S3. Deletes removed files. Metadata is tracked via `metadata.json`.

### Part 2: Fetch DataUSA API
Pulls population data from the DataUSA API and stores the JSON in S3.

### Part 3: Analytics
Loads the BLS `.Current` file and DataUSA JSON, then:
- Calculates population mean and standard deviation from 2013–2018.
- Finds the best year per `series_id` based on total quarterly values.
- Merges series PRS30006032 Q01 with population data.

### Part 4: Infrastructure-as-Code
The entire pipeline is orchestrated using AWS CDK:
- Step Functions trigger BLS and DataUSA lambdas daily.
- An SQS queue is triggered when new population JSON is saved.
- A reporting Lambda listens to SQS and logs insights.

---

## ⚙️ Getting Started

### Prerequisites

- Python 3.11+
- Node.js v16+
- AWS CLI v2
- AWS CDK v2
- Docker (Required for building Lambda dependencies using AWS CDK)

---

## 🛠️ Installation Steps

### 1. Install AWS CLI

```bash
# macOS/Linux
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

**Windows** [Download the AWS CLI MSI installer](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-windows.html)
```

Configure your credentials:

```bash
aws configure
```

---

### 2. Install AWS CDK

```bash
npm install -g aws-cdk
```

Confirm install:

```bash
cdk --version
```

---

### 3. Set Up the Project

```bash
git clone https://github.com/your-org/rearc-quest.git
cd rearc-quest

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate    # or venv\Scripts\activate on Windows

# Install CDK + Python Lambda dependencies
pip install -r requirements.txt
```

---

## 🧱 Deployment Instructions

### Bootstrap (first time only)

```bash
cdk bootstrap
```

### Deploy the stack

```bash
cdk deploy
```

The stack deploys:
- An S3 bucket with public read access
- Three Lambda functions
- A Step Function triggered daily at 6AM EST
- An SQS queue triggered by new population data
- CloudWatch logging for Lambda output

---

## 📊 Running Local Analysis

Use `analysis.ipynb` to:
- Re-run or prototype analytics from `report_generator.py`
- Load data from S3 into PySpark or Pandas
- Validate outputs before production runs
- Demonstrate multi-language capability using both Pandas and PySpark for identical analysis

To run:

```bash
jupyter notebook analysis.ipynb
```

---

## 📌 Notes

- The BLS file sync Lambda uses a `User-Agent` header to comply with BLS scraping guidelines.
- CDK generates the bucket name dynamically as:  
  `rearc-quest-<account-id>-<region>`
- JSON and CSV data are stored under:
  - `bls/files/`
  - `datausa/population.json`

---

## ✅ Requirements Demonstrated

| Skill                            | Demonstrated? |
|----------------------------------|---------------|
| Data Engineering                 | ✅             |
| AWS Lambda                       | ✅             |
| AWS S3 / Boto3                   | ✅             |
| AWS CDK / Infrastructure-as-Code| ✅             |
| Step Functions / Automation      | ✅             |
| Pandas / PySpark                 | ✅             |
| SQS Integration                  | ✅             |

---

## 👨‍💻 Author

Brandon Young  
[brandon@jsbsolutions.io](mailto:brandon@jsbsolutions.io)

---

## License

This project is for Rearc technical evaluation purposes only. Do not redistribute.
