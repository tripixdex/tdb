import SwiftUI

enum AppTheme: String, CaseIterable, Identifiable {
    case apple = "Apple"
    case aero  = "Aero"
    case blade = "Blade"
    var id: String { rawValue }
}

struct DS {
    let theme: AppTheme

    static func p3(_ r: Double, _ g: Double, _ b: Double, _ a: Double = 1) -> Color {
        Color(.displayP3, red: r, green: g, blue: b, opacity: a)
    }

    // Layout
    var radiusXL: CGFloat { 22 }
    var radiusL: CGFloat  { 18 }
    var radiusM: CGFloat  { 14 }
    var strokeThin: CGFloat { 1 }

    // Accent
    var accent: Color {
        switch theme {
        case .apple: return .accentColor
        case .aero:  return DS.p3(0.12, 0.72, 1.00, 1.0)   // glossy cyan
        case .blade: return DS.p3(0.78, 0.38, 1.00, 1.0)   // violet neon
        }
    }

    // Background
    var bgGradient: LinearGradient {
        switch theme {
        case .apple:
            return LinearGradient(colors: [
                DS.p3(0.98, 0.98, 0.99, 1),
                DS.p3(0.95, 0.96, 0.98, 1)
            ], startPoint: .topLeading, endPoint: .bottomTrailing)

        case .aero:
            return LinearGradient(colors: [
                DS.p3(0.93, 0.98, 1.00, 1),
                DS.p3(0.78, 0.92, 1.00, 1),
                DS.p3(0.92, 0.96, 1.00, 1)
            ], startPoint: .topLeading, endPoint: .bottomTrailing)

        case .blade:
            return LinearGradient(colors: [
                DS.p3(0.06, 0.06, 0.10, 1),
                DS.p3(0.03, 0.04, 0.06, 1)
            ], startPoint: .top, endPoint: .bottom)
        }
    }

    // Card chrome
    var cardFill: AnyShapeStyle { AnyShapeStyle(.ultraThinMaterial) }

    var cardStroke: Color {
        switch theme {
        case .apple: return .white.opacity(0.26)
        case .aero:  return DS.p3(0.90, 0.98, 1.00, 0.55)
        case .blade: return .white.opacity(0.18)
        }
    }

    var subtleShadow: Color {
        switch theme {
        case .apple: return .black.opacity(0.10)
        case .aero:  return .black.opacity(0.12)
        case .blade: return .black.opacity(0.35)
        }
    }

    // Grid
    var gridStripe: Color {
        switch theme {
        case .apple: return .black.opacity(0.03)
        case .aero:  return DS.p3(0.10, 0.45, 0.90, 0.06)
        case .blade: return .white.opacity(0.03)
        }
    }

    var gridHeader: Color {
        switch theme {
        case .apple: return .black.opacity(0.04)
        case .aero:  return DS.p3(0.10, 0.55, 0.95, 0.10)
        case .blade: return .white.opacity(0.04)
        }
    }

    // Noise intensity
    var noiseOpacity: Double {
        switch theme {
        case .apple: return 0.025
        case .aero:  return 0.040
        case .blade: return 0.050
        }
    }
}

struct AppBackground: View {
    let ds: DS

    var body: some View {
        ZStack {
            ds.bgGradient.ignoresSafeArea()

            // floating blobs (Aero/Y2K depth)
            ZStack {
                Circle()
                    .fill(ds.accent.opacity(ds.theme == .blade ? 0.22 : 0.18))
                    .frame(width: 520, height: 520)
                    .blur(radius: 70)
                    .offset(x: 240, y: -220)

                Circle()
                    .fill(DS.p3(0.10, 1.00, 0.78, ds.theme == .blade ? 0.10 : 0.14))
                    .frame(width: 420, height: 420)
                    .blur(radius: 90)
                    .offset(x: -260, y: -80)

                Circle()
                    .fill(DS.p3(1.00, 0.55, 0.88, ds.theme == .blade ? 0.12 : 0.10))
                    .frame(width: 520, height: 520)
                    .blur(radius: 110)
                    .offset(x: -120, y: 260)
            }
            .blendMode(.screen)
            .opacity(ds.theme == .apple ? 0.65 : 1.0)
            .ignoresSafeArea()

            // vignette
            RadialGradient(colors: [
                .clear,
                .black.opacity(ds.theme == .blade ? 0.55 : 0.14)
            ], center: .center, startRadius: 260, endRadius: 980)
            .ignoresSafeArea()

            // grain
            NoiseOverlay(opacity: ds.noiseOpacity)
                .ignoresSafeArea()
        }
    }
}
