#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/venv"
MIN_PYTHON="3.8"

check_python() {
    local cmd
    for cmd in python3 python; do
        if command -v "$cmd" &>/dev/null; then
            local version
            version=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
            if "$cmd" -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" 2>/dev/null; then
                echo "$cmd"
                return 0
            fi
        fi
    done
    echo "Erro: Python ${MIN_PYTHON}+ não encontrado." >&2
    exit 1
}

PYTHON=$(check_python)

if [ ! -d "${VENV_DIR}" ]; then
    echo "Criando virtualenv em ${VENV_DIR}..."
    "${PYTHON}" -m venv "${VENV_DIR}"
    echo "Instalando dependências..."
    "${VENV_DIR}/bin/pip" install --quiet --upgrade pip
    "${VENV_DIR}/bin/pip" install --quiet -r "${SCRIPT_DIR}/requirements.txt"
    echo "Instalação concluída."
    echo ""
fi

exec "${VENV_DIR}/bin/python" "${SCRIPT_DIR}/main.py" "$@"
