import SwiftUI

struct GlassCard<Content: View>: View {
    let ds: DS
    let content: Content

    init(ds: DS, @ViewBuilder content: () -> Content) {
        self.ds = ds
        self.content = content()
    }

    var body: some View {
        content
            .padding(12)
            .background(ds.cardFill)
            .overlay(SpecularOverlay().opacity(ds.theme == .apple ? 0.55 : 0.85))
            .clipShape(RoundedRectangle(cornerRadius: ds.radiusXL, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: ds.radiusXL, style: .continuous)
                    .stroke(ds.cardStroke, lineWidth: ds.strokeThin)
            )
            .overlay(
                InnerGlow(color: ds.accent).opacity(ds.theme == .blade ? 0.55 : 0.35)
                    .clipShape(RoundedRectangle(cornerRadius: ds.radiusXL, style: .continuous))
            )
            .shadow(color: ds.subtleShadow, radius: 18, x: 0, y: 8)
    }
}

struct PillButtonStyle: ButtonStyle {
    let ds: DS
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.system(.body, design: .rounded).weight(.semibold))
            .padding(.horizontal, 12)
            .padding(.vertical, 7)
            .background(.thinMaterial)
            .overlay(SpecularOverlay().opacity(0.8))
            .clipShape(Capsule())
            .overlay(Capsule().stroke(ds.cardStroke, lineWidth: ds.strokeThin))
            .scaleEffect(configuration.isPressed ? 0.98 : 1.0)
            .animation(.snappy(duration: 0.12), value: configuration.isPressed)
    }
}
