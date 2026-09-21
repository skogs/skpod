class Skpod < Formula
  desc "Inter-agent communication for local coding sessions"
  homepage "https://github.com/skogs/skpod"
  version "0.1.0"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/skogs/skpod/releases/download/v#{version}/skpod_#{version}_darwin_arm64.tar.gz"
      sha256 "0e0b4f8f9e657d093ad11f8175b94b9faf0d49fb10c165fc9a1d905e4de9d85f"
    else
      url "https://github.com/skogs/skpod/releases/download/v#{version}/skpod_#{version}_darwin_amd64.tar.gz"
      sha256 "d69b9e0d255cdbe2b4ccdecc10e06f82073f138d3f09f51bbc14250ea775c617"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/skogs/skpod/releases/download/v#{version}/skpod_#{version}_linux_arm64.tar.gz"
      sha256 "a201c725850192c29b0b34237b901ab6cf8116c7f5f2fde44563c836e12316a8"
    else
      url "https://github.com/skogs/skpod/releases/download/v#{version}/skpod_#{version}_linux_amd64.tar.gz"
      sha256 "fe55fbeedcadfd35df5f6d8396ba195cf4c3d7d91baa28b2b29583579e61b64b"
    end
  end

  def install
    bin.install "skpod"
  end

  test do
    assert_match "skpod", shell_output("#{bin}/skpod version")
  end
end
