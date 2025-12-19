import SwiftUI
import AppKit

final class NoiseTexture {
    static let shared = NoiseTexture()
    let image: NSImage

    private init() {
        let w = 128
        let h = 128
        let bytesPerPixel = 4
        let bytesPerRow = w * bytesPerPixel
        var data = [UInt8](repeating: 0, count: h * bytesPerRow)

        for y in 0..<h {
            for x in 0..<w {
                let i = y * bytesPerRow + x * bytesPerPixel
                let v = UInt8.random(in: 0...255)
                data[i + 0] = v
                data[i + 1] = v
                data[i + 2] = v
                data[i + 3] = 255
            }
        }

        let cs = CGColorSpaceCreateDeviceRGB()
        let ctx = CGContext(
            data: &data,
            width: w,
            height: h,
            bitsPerComponent: 8,
            bytesPerRow: bytesPerRow,
            space: cs,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        )!

        let cg = ctx.makeImage()!
        self.image = NSImage(cgImage: cg, size: NSSize(width: w, height: h))
    }
}

struct NoiseOverlay: View {
    let opacity: Double
    var body: some View {
        Image(nsImage: NoiseTexture.shared.image)
            .resizable(resizingMode: .tile)
            .interpolation(.none)
            .blendMode(.overlay)
            .opacity(opacity)
            .allowsHitTesting(false)
            .accessibilityHidden(true)
    }
}

struct SpecularOverlay: View {
    var body: some View {
        LinearGradient(colors: [
            Color.white.opacity(0.55),
            Color.white.opacity(0.12),
            Color.clear,
            Color.white.opacity(0.16)
        ], startPoint: .topLeading, endPoint: .bottomTrailing)
        .blendMode(.softLight)
        .allowsHitTesting(false)
        .accessibilityHidden(true)
    }
}

struct InnerGlow: View {
    let color: Color
    var body: some View {
        RoundedRectangle(cornerRadius: 22, style: .continuous)
            .stroke(color.opacity(0.35), lineWidth: 1)
            .blur(radius: 1.2)
            .allowsHitTesting(false)
            .accessibilityHidden(true)
    }
}
