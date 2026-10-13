// swift-tools-version:5.9
// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

import PackageDescription

// Local-development twin of the root Package.swift: points at the xcframework
// built by `scripts/build-ios-xcframework.sh --swift-out Sources/MatrixRtc`
// instead of a released zip. Copy it over the root manifest (and do not commit
// the result) to add the repository as a local package in Xcode.

// Keep identical to the root Package.swift, and above `package` (a top-level
// constant read before its declaration is silently empty in a manifest).
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
        .library(name: "MatrixRtc", targets: ["MatrixRtc"]),
    ],
    targets: [
        .binaryTarget(name: "MatrixRtcFFI", path: "mobile/ios/build/MatrixRtcFFI.xcframework"),
        .target(
            name: "MatrixRtc",
            dependencies: ["MatrixRtcFFI"],
            linkerSettings: matrixRtcLinkerSettings
        ),
    ]
)
