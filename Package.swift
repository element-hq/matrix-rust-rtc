// swift-tools-version:5.9
// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

import PackageDescription

// Swift package for the released MatrixRtcFFI.xcframework (media variant:
// device + Apple Silicon simulator). The two constants below are rewritten by
// scripts/update-package-swift.sh from the release workflow, which then tags the
// commit; the zip is the asset attached to that tag's GitHub release, and
// Sources/MatrixRtc holds the Swift generated for that same build.
//
// Add it in Xcode with this repository's URL at a `v*` tag, then add `-ObjC` to
// the app target's "Other Linker Flags" (libwebrtc's Objective-C categories are
// otherwise dead-stripped from the static archive). See mobile/PACKAGING.md.
//
// For a local build, copy mobile/ios/Debug-Package.swift over this file.
let version = "0.4.0-rc.1"
let checksum = "56e11948f99fb309483794719d4f2f4fa9de225ddc7f364aac6a8fc868179ebc"
let url = "https://github.com/element-hq/matrix-rust-rtc/releases/download/v\(version)/MatrixRtcFFI.xcframework.zip"

// What the statically linked libwebrtc inside MatrixRtcFFI.xcframework needs
// from the system. Declared here so a consumer's link succeeds without knowing
// libwebrtc's internals; SwiftPM propagates these to the app's link step.
// (Must precede `package`: a manifest is script-mode Swift, and a top-level
// constant read before its declaration is silently empty.)
let matrixRtcLinkerSettings: [LinkerSetting] = [
    .linkedFramework("AVFoundation"),
    .linkedFramework("AudioToolbox"),
    .linkedFramework("CoreMedia"),
    .linkedFramework("CoreVideo"),
    .linkedFramework("VideoToolbox"),
    .linkedFramework("Metal"),
    .linkedFramework("MetalKit"),
    .linkedFramework("QuartzCore"),
    .linkedFramework("CoreGraphics"),
    .linkedFramework("Network"),
    .linkedFramework("UIKit"),
    .linkedLibrary("c++"),
]

let package = Package(
    name: "MatrixRtc",
    platforms: [
        .iOS(.v16),
    ],
    products: [
        // Static (the default): the archive is linked into the app target, so
        // the app's own `-ObjC` flag applies to it. A dynamic product would
        // link the archive into a framework without that flag.
        .library(name: "MatrixRtc", targets: ["MatrixRtc"]),
    ],
    targets: [
        .binaryTarget(name: "MatrixRtcFFI", url: url, checksum: checksum),
        .target(
            name: "MatrixRtc",
            dependencies: ["MatrixRtcFFI"],
            linkerSettings: matrixRtcLinkerSettings
        ),
    ]
)
