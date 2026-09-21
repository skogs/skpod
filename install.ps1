# skpod installer for Windows PowerShell
# Usage: irm https://raw.githubusercontent.com/skogs/skpod/main/install.ps1 | iex

$ErrorActionPreference = 'Stop'
$repo = "skogs/skpod"

Write-Host "Finding latest release of skpod for Windows..." -ForegroundColor Cyan

$releaseUrl = "https://api.github.com/repos/$repo/releases/latest"
try {
    $response = Invoke-RestMethod -Uri $releaseUrl -Headers @{ "User-Agent" = "skpod-installer" }
    $tag = $response.tag_name
} catch {
    # Fallback to redirect resolution
    $req = [System.Net.WebRequest]::Create("https://github.com/$repo/releases/latest")
    $req.AllowAutoRedirect = $false
    $res = $req.GetResponse()
    $redirectUrl = $res.GetResponseHeader("Location")
    $tag = $redirectUrl.Substring($redirectUrl.LastIndexOf("/") + 1)
}

if (-not $tag) {
    Write-Error "Could not resolve latest release version from GitHub."
    exit 1
}

$version = $tag.TrimStart("v")
$arch = "amd64"
$archiveName = "skpod_${version}_windows_${arch}.zip"
$downloadUrl = "https://github.com/$repo/releases/download/$tag/$archiveName"

$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

try {
    $zipPath = Join-Path $tmpDir $archiveName
    Write-Host "Downloading $downloadUrl..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri $downloadUrl -OutFile $zipPath -UseBasicParsing

    Write-Host "Extracting archive..." -ForegroundColor Cyan
    Expand-Archive -Path $zipPath -DestinationPath $tmpDir -Force

    $binary = Get-ChildItem -Path $tmpDir -Filter "skpod.exe" -Recurse | Select-Object -First 1
    if (-not $binary) {
        Write-Error "Archive did not contain skpod.exe"
        exit 1
    }
    $binaryPath = $binary.FullName

    $installDir = Join-Path $env:LOCALAPPDATA "Programs\skpod"
    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
    Copy-Item -Path $binaryPath -Destination (Join-Path $installDir "skpod.exe") -Force

    Write-Host "Successfully installed skpod to $installDir\skpod.exe" -ForegroundColor Green

    # Ensure installDir is in PATH
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($userPath -split ';' -notcontains $installDir) {
        Write-Host "Adding $installDir to User PATH..." -ForegroundColor Cyan
        [Environment]::SetEnvironmentVariable("Path", "$userPath;$installDir", "User")
        $env:Path = "$env:Path;$installDir"
    }

    # Verify
    & (Join-Path $installDir "skpod.exe") version
} finally {
    Remove-Item -Path $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}
