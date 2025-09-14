![CI](https://github.com/gradhouse/arxiv-bucket/actions/workflows/ci.yml/badge.svg)

# arxiv-bucket

## 1. Overview

arxiv-bucket is a python tool for accessing and processing arXiv's bulk data from AWS S3. It handles the download, extraction, validation, and basic cataloging of arXiv submission packages with minimal configuration. The library manages requester-pays S3 access, archive extraction, file validation, and creates usable metadata for downstream processing.

### 1.1 Key capabilities
- Reliable S3 integration and safe download helpers (wrapping aws CLI calls with retries, timeouts, and request-payer support).
- Bulk archive handling: pattern-based discovery, tar/tgz/gz extraction, and content validation against expected submission filename patterns.
- Submission validation: file-type detection, archive extraction checks, format-based heuristics (PDF/PS/TEX), and diagnostics reporting.
- Metadata extraction and hashing (MD5/SHA256) for robust registry keys and deduplication.
- Registries and diagnostics: in-memory registries with controlled update semantics, conflict logging, and test coverage for handlers.
- Extensible handler model: per-file-type handlers (archive, pdf, image, tex, xml) to keep the pipeline modular and easy to extend.

### 1.2 Quick usage notes
- Requires Python 3.12+, a virtual environment (venv), and AWS CLI configured for requester-pays access to arXiv S3.
- Typical flow: fetch manifest → copy bulk archive from S3 → validate archive → generate registry entry (metadata + SHA256 key).
- Optional dependencies (image handling, XML helpers, plotting for tests) are declared in the project dependencies.

## 2. Installation

### 2.1 Prerequisites

1. **Python 3.12+** is required
2. **Virtual environment (.venv)** must be created
2. **AWS CLI** must be installed and configured (see [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html))

### 2.2 Install Dependencies

```bash
# Install in development mode with test dependencies
pip install -e ".[test]"
```

### 2.3 AWS CLI Setup

#### 2.3.1 Install the AWS CLI

Follow the official AWS CLI installation instructions for your operating system:

- [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

This guide covers Windows, macOS, and Linux. Download the installer or use the provided commands as described in the documentation for your platform.

#### 2.3.2 Verify the Installation

Run the following command to verify the AWS CLI is installed:
```sh
aws --version
```

#### 2.3.3 Create or Find Your AWS Access Keys
1. Sign in to the [AWS Management Console](https://console.aws.amazon.com/).
2. Click your account name (top right) and select **Security credentials**.
3. Under **Access keys**, you can view, create, or manage your access keys.
4. If you do not have an access key, click **Create access key** and follow the prompts. Save the Access Key ID and Secret Access Key securely.

For more details, see the [Managing access keys](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).

#### 2.3.4 Configure the AWS CLI

Run the following command and follow the prompts:
```sh
aws configure
```
You will be asked for:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name (e.g., `us-east-1`)
- Default output format (e.g., `json`)

For more details, see the [official configuration guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).

#### 2.3.5 Test Your Configuration

Try listing your S3 buckets to confirm your setup:
```sh
aws s3 ls --request-payer requester s3://arxiv/src/
```

#### 2.3.6 Additional Resources
- [AWS CLI User Guide](https://docs.aws.amazon.com/cli/latest/userguide/)
- [AWS CLI Command Reference](https://docs.aws.amazon.com/cli/latest/reference/)

---
If you encounter issues, refer to the [AWS CLI troubleshooting guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-troubleshoot.html).


## 3. Disclaimer

This project is not affiliated with, endorsed by, or sponsored by arXiv or Cornell University.
All arXiv data and trademarks are the property of their respective owners.

Please refer to [arXiv’s license and terms of use](https://arxiv.org/help/license)
and their [bulk data policy](https://info.arxiv.org/help/bulk_data_s3.html) for more information.

## 4. License

Licensed under the MIT License. See the LICENSE file for more details.
