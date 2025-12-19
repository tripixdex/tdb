import SwiftUI

struct SidebarRow: View {
    let ds: DS
    let title: String
    let icon: String
    let selected: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            HStack(spacing: 10) {
                Image(systemName: icon)
                    .frame(width: 18)
                    .foregroundStyle(selected ? ds.accent : .secondary)

                Text(title)
                    .font(.system(.body, design: .rounded).weight(selected ? .semibold : .regular))
                    .foregroundStyle(.primary)

                Spacer(minLength: 0)
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background {
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .fill(selected ? ds.accent.opacity(ds.theme == .blade ? 0.18 : 0.12) : .clear)
            }
            .overlay {
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(selected ? ds.accent.opacity(0.22) : .clear, lineWidth: 1)
            }
        }
        .buttonStyle(.plain)
    }
}

func iconForTable(_ t: String) -> String {
    switch t.lowercased() {
    case "customer": return "person.2"
    case "deliveries": return "shippingbox"
    case "employees": return "person.badge.key"
    case "orders": return "cart"
    case "product": return "cube"
    case "warehouses": return "building.2"
    default: return "tablecells"
    }
}
