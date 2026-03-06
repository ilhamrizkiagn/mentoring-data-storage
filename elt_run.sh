#!/bin/bash

echo "========== Start Orcestration Process =========="

# Virtual Environment Path
VENV_PATH="/mnt/c/Users/Ilham/miniconda3/Scripts/activate"

# Activate Virtual Environment
source "$VENV_PATH"
conda activate ds-mentoring

# Set Python script
PYTHON_SCRIPT="/mnt/d/belajar_pacmann/data_storage/mentoring2/elt_main.py"

# Run Python Script 
python "$PYTHON_SCRIPT"


echo "========== End of Orcestration Process =========="