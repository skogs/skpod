#!/bin/sh
# skpod installer for macOS and Linux
# Usage: curl -fsSL https://raw.githubusercontent.com/skogs/skpod/main/install.sh | sh

set -eu

REPO="skogs/skpod"

# Detect OS
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
  darwin|linux) ;;
  msys*|mingw*|cygwin*)
    echo "Notice: Detected Windows environment ($OS)." >&2
    echo "Run this command in PowerShell to install skpod on Windows:" >&2
    echo "  irm https://raw.githubusercontent.com/$REPO/main/install.ps1 | iex" >&2
    exit 1
    ;;
  *)
    echo "Error: Unsupported operating system: $OS" >&2
    echo "For Windows, run in PowerShell:" >&2
    echo "  irm https://raw.githubusercontent.com/$REPO/main/install.ps1 | iex" >&2
    exit 1
    ;;
esac

# Detect Architecture
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)
    ARCH="amd64"
    ;;
  arm64|aarch64)
    ARCH="arm64"
    ;;
  *)
    echo "Error: Unsupported architecture: $ARCH" >&2
    exit 1
    ;;
esac

# Check dependencies
command -v curl >/dev/null 2>&1 || { echo "Error: curl is required to install skpod." >&2; exit 1; }
command -v tar >/dev/null 2>&1 || { echo "Error: tar is required to install skpod." >&2; exit 1; }

echo "Finding latest release of skpod for $OS/$ARCH..."

# Resolve latest release tag (fallback to redirect resolution if API is rate-limited)
TAG=""
if TAG=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'); then
  :
fi
if [ -z "$TAG" ]; then
  # Fallback: follow redirect of releases/latest
  LATEST_URL=$(curl -fsSLI -o /dev/null -w "%{url_effective}" "https://github.com/$REPO/releases/latest" 2>/dev/null || true)
  TAG="${LATEST_URL##*/}"
fi

if [ -z "$TAG" ] || [ "$TAG" = "latest" ]; then
  echo "Error: Could not determine latest release version from GitHub." >&2
  exit 1
fi

VERSION="${TAG#v}"
ARCHIVE_NAME="skpod_${VERSION}_${OS}_${ARCH}.tar.gz"
DOWNLOAD_URL="https://github.com/$REPO/releases/download/${TAG}/${ARCHIVE_NAME}"

# Temporary workspace
TMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t 'skpod')"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Downloading $DOWNLOAD_URL..."
curl -fSL "$DOWNLOAD_URL" -o "$TMP_DIR/$ARCHIVE_NAME"

# Extract
tar -xzf "$TMP_DIR/$ARCHIVE_NAME" -C "$TMP_DIR"

BINARY_PATH=$(find "$TMP_DIR" -type f -name skpod | head -n 1)
if [ -z "$BINARY_PATH" ] || [ ! -f "$BINARY_PATH" ]; then
  echo "Error: Archive did not contain skpod binary." >&2
  exit 1
fi

chmod +x "$BINARY_PATH"

# Choose install directory
if [ -n "${SKPOD_INSTALL_DIR:-}" ]; then
  INSTALL_DIR="$SKPOD_INSTALL_DIR"
elif [ -w "/usr/local/bin" ]; then
  INSTALL_DIR="/usr/local/bin"
else
  INSTALL_DIR="$HOME/.local/bin"
fi

mkdir -p "$INSTALL_DIR"

# Move binary
if [ -w "$INSTALL_DIR" ]; then
  mv "$BINARY_PATH" "$INSTALL_DIR/skpod"
else
  echo "Elevated permissions required to write to $INSTALL_DIR:"
  sudo mv "$BINARY_PATH" "$INSTALL_DIR/skpod"
fi

echo "Successfully installed skpod to $INSTALL_DIR/skpod"

# Check PATH
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo ""
    echo "Notice: $INSTALL_DIR is not in your PATH."
    echo "To add it, append this to your ~/.zshrc or ~/.bashrc:"
    echo "  export PATH=\"\$PATH:$INSTALL_DIR\""
    echo ""
    ;;
esac

# Verify
"$INSTALL_DIR/skpod" version
