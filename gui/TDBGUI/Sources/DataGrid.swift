import SwiftUI

private extension View {
    @ViewBuilder
    func monospacedDigits(_ enabled: Bool) -> some View {
        if enabled { self.monospacedDigit() } else { self }
    }
}

struct DataGrid: View {
    let columns: [String]
    let rows: [[String]]
    let ds: DS

    private func isNumber(_ s: String) -> Bool {
        Double(s.replacingOccurrences(of: ",", with: ".")) != nil
    }

    var body: some View {
        ScrollView([.vertical, .horizontal]) {
            LazyVStack(alignment: .leading, spacing: 0, pinnedViews: [.sectionHeaders]) {
                Section(header: header) {
                    ForEach(rows.indices, id: \.self) { i in
                        rowView(i)
                            .background(i.isMultiple(of: 2) ? ds.gridStripe : Color.clear)
                    }
                }
            }
            .clipShape(RoundedRectangle(cornerRadius: ds.radiusM, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: ds.radiusM, style: .continuous)
                    .stroke(ds.cardStroke, lineWidth: ds.strokeThin)
            )
        }
    }

    private var header: some View {
        HStack(spacing: 0) {
            ForEach(columns, id: \.self) { c in
                Text(c)
                    .font(.system(.footnote, design: .rounded).weight(.semibold))
                    .foregroundStyle(.secondary)
                    .frame(minWidth: 140, alignment: .leading)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 8)
                    .overlay(divider, alignment: .trailing)
            }
        }
        .background(ds.gridHeader)
    }

    private func rowView(_ i: Int) -> some View {
        HStack(spacing: 0) {
            let row = rows[i]
            ForEach(columns.indices, id: \.self) { j in
                let v = j < row.count ? row[j] : ""
                let numeric = isNumber(v)

                Text(v)
                    .font(.system(.body, design: .rounded))
                    .monospacedDigits(numeric)
                    .frame(minWidth: 140, alignment: numeric ? .trailing : .leading)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 7)
                    .overlay(divider, alignment: .trailing)
            }
        }
    }

    private var divider: some View {
        Rectangle().fill(ds.cardStroke.opacity(0.55)).frame(width: 1)
    }
}
