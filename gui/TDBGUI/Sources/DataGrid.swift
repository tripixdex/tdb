import SwiftUI

struct DataGrid: View {
    let columns: [String]
    let rows: [[String]]

    var body: some View {
        ScrollView([.vertical, .horizontal]) {
            LazyVStack(alignment: .leading, spacing: 0) {
                header
                Divider()
                ForEach(rows.indices, id: \.self) { i in
                    rowView(rows[i])
                    Divider()
                }
            }
            .padding(8)
        }
        .background(.background)
        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .stroke(.quaternary, lineWidth: 1)
        )
    }

    private var header: some View {
        HStack(spacing: 0) {
            ForEach(columns, id: \.self) { c in
                Text(c)
                    .font(.system(.subheadline, design: .rounded).weight(.semibold))
                    .foregroundStyle(.secondary)
                    .frame(minWidth: 160, maxWidth: 260, alignment: .leading)
                    .padding(.vertical, 6)
                    .padding(.horizontal, 8)
            }
        }
    }

    private func rowView(_ r: [String]) -> some View {
        HStack(spacing: 0) {
            ForEach(columns.indices, id: \.self) { idx in
                Text(idx < r.count ? r[idx] : "")
                    .font(.system(.body, design: .monospaced))
                    .lineLimit(2)
                    .frame(minWidth: 160, maxWidth: 260, alignment: .leading)
                    .padding(.vertical, 6)
                    .padding(.horizontal, 8)
            }
        }
    }
}
