#!/bin/bash
# =============================================================================
# DATA LAKEHOUSE - SETUP SCRIPT
# =============================================================================
# Script khởi tạo môi trường Data Lakehouse
# =============================================================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║          DATA LAKEHOUSE - INITIAL SETUP                        ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check Docker
echo -e "${YELLOW}Checking Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo -e "${RED}Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker is ready${NC}"

# Check Docker Compose
echo -e "${YELLOW}Checking Docker Compose...${NC}"
if command -v docker-compose &> /dev/null; then
    echo -e "${GREEN}✓ Docker Compose is installed${NC}"
elif docker compose version &> /dev/null; then
    echo -e "${GREEN}✓ Docker Compose (v2) is installed${NC}"
else
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose.${NC}"
    exit 1
fi

# Check system resources
echo ""
echo -e "${YELLOW}Checking system resources...${NC}"

# Check RAM
TOTAL_RAM=$(free -g | awk '/^Mem:/{print $2}')
if [ "$TOTAL_RAM" -lt 8 ]; then
    echo -e "${YELLOW}⚠️ Warning: System has ${TOTAL_RAM}GB RAM. Recommended: 16GB${NC}"
else
    echo -e "${GREEN}✓ RAM: ${TOTAL_RAM}GB${NC}"
fi

# Check disk space
DISK_SPACE=$(df -BG . | awk 'NR==2 {print $4}' | sed 's/G//')
if [ "$DISK_SPACE" -lt 20 ]; then
    echo -e "${YELLOW}⚠️ Warning: Only ${DISK_SPACE}GB disk space available. Recommended: 20GB+${NC}"
else
    echo -e "${GREEN}✓ Disk space: ${DISK_SPACE}GB available${NC}"
fi

# Create directories
echo ""
echo -e "${YELLOW}Creating directories...${NC}"
mkdir -p data/raw
mkdir -p notebooks
mkdir -p logs
echo -e "${GREEN}✓ Directories created${NC}"

# Check for data files
echo ""
echo -e "${YELLOW}Checking for data files...${NC}"
if [ -z "$(ls -A data/raw 2>/dev/null)" ]; then
    echo -e "${YELLOW}⚠️ No data files found in data/raw/${NC}"
    echo ""
    pip install kaggle
    echo "Downloading the dataset using Kaggle CLI:"
    kaggle datasets download -d mkechinov/ecommerce-events-history-in-cosmetics-shop -p data/raw/ --unzip
    echo "Please download the dataset from Kaggle:"
    echo "https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop"
    echo ""
    echo "Then place the CSV files in: data/raw/"
else
    echo -e "${GREEN}✓ Data files found in data/raw/${NC}"
fi

# Build Docker images
echo ""
echo -e "${YELLOW}Building Docker images...${NC}"
cd docker
if command -v docker-compose &> /dev/null; then
    docker-compose build
elif docker compose version &> /dev/null; then
    docker compose build
else
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose.${NC}"
    exit 1
fi
cd ..
echo -e "${GREEN}✓ Docker images built${NC}"

# Done
echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    SETUP COMPLETE!                             ║${NC}"
echo -e "${GREEN}╠═══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  Next steps:                                                   ║${NC}"
echo -e "${GREEN}║  1. Download data (if not done):  See instructions above       ║${NC}"
echo -e "${GREEN}║  2. Start services:               make up                      ║${NC}"
echo -e "${GREEN}║  3. Run pipeline:                 make pipeline-full           ║${NC}"
echo -e "${GREEN}║  4. Setup Superset:               make setup-superset          ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
