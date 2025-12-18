import SwiftUI
import UniformTypeIdentifiers

enum MainTab: String, CaseIterable, Identifiable {
    case preview = "Preview"
    case schema = "Schema"
    case sql = "SQL"
    case validate = "Validate"
    var id: String { rawValue }
}

@MainActor
final class AppVM: ObservableObject {
    @Published var projectURL: URL?
    @Published var dbRelPath: String = "build/school.duckdb"

    @Published var tables: [String] = []
    @Published var selectedTable: String?

    @Published var tab: MainTab = .preview
    @Published var isBusy: Bool = false
    @Published var status: String = "ready"
    @Published var showProjectPicker: Bool = false

    // Preview
    @Published var previewColumns: [String] = []
    @Published var previewRows: [[String]] = []
    @Published var previewLimit: Int = 50
    @Published var previewOffset: Int = 0
    @Published var previewTotal: Int? = nil

    // Schema
    @Published var schemaColumns: [String] = ["column_name", "column_type", "null", "key"]
    @Published var schemaRows: [[String]] = []

    // SQL
    @Published var query: String = "SELECT * FROM customer LIMIT 50;"
    @Published var sqlColumns: [String] = []
    @Published var sqlRows: [[String]] = []

    // Validate
    @Published var pkColumns: [String] = ["table", "pk", "n", "distinct", "null", "dup"]
    @Published var pkRows: [[String]] = []
    @Published var fkColumns: [String] = ["fk", "orphans"]
    @Published var fkRows: [[String]] = []

    private func runner() throws -> TdbRunner {
        guard let url = projectURL else { throw TdbError(message: "Project folder not selected") }
        return TdbRunner(projectURL: url)
    }

    private func dbPathArg() throws -> [String] {
        guard let url = projectURL else { throw TdbError(message: "Project folder not selected") }
        let dbURL = url.appendingPathComponent(dbRelPath)
        return ["--db", dbURL.path]
    }

    private func setBusy(_ busy: Bool, _ msg: String) {
        isBusy = busy
        status = msg
    }

    func pickProject(_ url: URL) {
        projectURL = url
        UserDefaults.standard.set(url.path, forKey: "tdb.projectPath")
        status = "project: \(url.path)"
        Task { await refreshTables() }
    }

    func tryRestoreProject() {
        let path = UserDefaults.standard.string(forKey: "tdb.projectPath") ?? ""
        guard !path.isEmpty else { return }
        let url = URL(fileURLWithPath: path, isDirectory: true)
        projectURL = url
        status = "project: \(url.path)"
        Task { await refreshTables() }
    }

    func refreshTables() async {
        do {
            setBusy(true, "refresh…")
            defer { setBusy(false, status) }

            let r = try runner()
            let args = ["tables"] + (try dbPathArg()) + ["--json"]
            let any = try await r.runJSON(args)
            let names = (any as? [String]) ?? []
            tables = names.sorted()

            if selectedTable == nil { selectedTable = tables.first }
            if let t = selectedTable {
                previewOffset = 0
                await loadPreview(t)
                await loadSchema(t)
            }

            status = "tables: \(tables.count)"
        } catch {
            setBusy(false, "\(error)")
        }
    }

    func buildDB() async {
        do {
            setBusy(true, "build…")
            defer { setBusy(false, status) }

            let r = try runner()
            guard let url = projectURL else { throw TdbError(message: "Project folder not selected") }

            let folder = url.appendingPathComponent("data/raw")
            let dbURL = url.appendingPathComponent(dbRelPath)
            let profile = url.appendingPathComponent(".tdb_profile.json")

            let args = ["build", folder.path, "--db", dbURL.path, "--profile", profile.path, "--json"]
            _ = try await r.runJSON(args)

            status = "build ok: \(dbRelPath)"
            await refreshTables()
        } catch {
            setBusy(false, "\(error)")
        }
    }

    func validateDB() async {
        do {
            setBusy(true, "validate…")
            defer { setBusy(false, status) }

            let r = try runner()
            guard let url = projectURL else { throw TdbError(message: "Project folder not selected") }

            let dbURL = url.appendingPathComponent(dbRelPath)
            let profile = url.appendingPathComponent(".tdb_profile.json")

            let args = ["validate", "--db", dbURL.path, "--profile", profile.path, "--json"]
            let any = try await r.runJSON(args)
            let dict = (any as? [String: Any]) ?? [:]

            let pk = (dict["pk"] as? [[String: Any]]) ?? []
            let fk = (dict["fk"] as? [[String: Any]]) ?? []

            pkRows = pk.map {
                [
                    String(describing: $0["table"] ?? ""),
                    ( ($0["pk"] as? [String]) ?? [] ).joined(separator: ","),
                    String(describing: $0["n"] ?? ""),
                    String(describing: $0["distinct"] ?? ""),
                    String(describing: $0["null"] ?? ""),
                    String(describing: $0["dup"] ?? "")
                ]
            }

            fkRows = fk.map {
                [
                    String(describing: $0["fk"] ?? "").replacingOccurrences(of: "->", with: "→"),
                    String(describing: $0["orphans"] ?? "")
                ]
            }

            status = "validate ok: pk=\(pkRows.count), fk=\(fkRows.count)"
            tab = .validate
        } catch {
            setBusy(false, "\(error)")
        }
    }

    func loadSchema(_ table: String) async {
        do {
            let r = try runner()
            let args = ["describe", "--table", table] + (try dbPathArg()) + ["--json"]
            let any = try await r.runJSON(args)
            let rows = (any as? [[String: Any]]) ?? []

            // DESCRIBE returns stable fields; show the most useful subset
            schemaRows = rows.map {
                [
                    String(describing: $0["column_name"] ?? ""),
                    String(describing: $0["column_type"] ?? ""),
                    String(describing: $0["null"] ?? ""),
                    String(describing: $0["key"] ?? "")
                ]
            }
        } catch {
            status = "\(error)"
        }
    }

    func loadPreview(_ table: String) async {
        do {
            let r = try runner()

            // total count for proper paging
            let countQ = "SELECT COUNT(*) AS n FROM \"\(table)\";"
            let countAny = try await r.runJSON(["sql", countQ] + (try dbPathArg()) + ["--json"])
            if let d = countAny as? [String: Any],
               let rr = d["rows"] as? [[Any]],
               let first = rr.first?.first as? Int {
                previewTotal = first
            }

            let q = "SELECT * FROM \"\(table)\" LIMIT \(previewLimit) OFFSET \(previewOffset);"
            let any = try await r.runJSON(["sql", q] + (try dbPathArg()) + ["--json"])
            let dict = (any as? [String: Any]) ?? [:]
            let cols = (dict["columns"] as? [String]) ?? []
            let rowsAny = (dict["rows"] as? [[Any]]) ?? []

            previewColumns = cols
            previewRows = rowsAny.map { $0.map { String(describing: $0) } }

            let totalStr = previewTotal.map { " / \($0)" } ?? ""
            status = "preview: \(table) rows \(previewOffset + 1)-\(previewOffset + previewRows.count)\(totalStr)"
        } catch {
            status = "\(error)"
        }
    }

    func previewPrev() async {
        previewOffset = max(0, previewOffset - previewLimit)
        if let t = selectedTable { await loadPreview(t) }
    }

    func previewNext() async {
        let next = previewOffset + previewLimit
        if let total = previewTotal, next >= total { return }
        previewOffset = next
        if let t = selectedTable { await loadPreview(t) }
    }

    func runSQL() async {
        do {
            setBusy(true, "sql…")
            defer { setBusy(false, status) }

            let r = try runner()
            let args = ["sql", query] + (try dbPathArg()) + ["--json"]
            let any = try await r.runJSON(args)
            let dict = (any as? [String: Any]) ?? [:]
            let cols = (dict["columns"] as? [String]) ?? []
            let rowsAny = (dict["rows"] as? [[Any]]) ?? []
            sqlColumns = cols
            sqlRows = rowsAny.map { $0.map { String(describing: $0) } }
            let ms = (dict["ms"] as? Double) ?? 0
            status = String(format: "sql: %.1f ms, rows: %d", ms, sqlRows.count)
        } catch {
            setBusy(false, "\(error)")
        }
    }
}

struct ContentView: View {
    @StateObject private var vm = AppVM()

    var body: some View {
        NavigationSplitView {
            List(selection: $vm.selectedTable) {
                ForEach(vm.tables, id: \.self) { t in
                    Text(t)
                        .font(.system(.body, design: .rounded))
                        .tag(Optional(t))
                }
            }
            .navigationTitle("Tables")
            .onChange(of: vm.selectedTable) { _, newValue in
                guard let t = newValue else { return }
                vm.previewOffset = 0
                Task {
                    await vm.loadPreview(t)
                    await vm.loadSchema(t)
                }
            }
        } detail: {
            VStack(spacing: 12) {
                topBar

                Picker("", selection: $vm.tab) {
                    ForEach(MainTab.allCases) { t in
                        Text(t.rawValue).tag(t)
                    }
                }
                .pickerStyle(.segmented)

                Group {
                    switch vm.tab {
                    case .preview: previewPane
                    case .schema: schemaPane
                    case .sql: sqlPane
                    case .validate: validatePane
                    }
                }

                bottomStatus
            }
            .padding(12)
            .navigationTitle("TDB")
            .onAppear { vm.tryRestoreProject() }
            .fileImporter(
                isPresented: $vm.showProjectPicker,
                allowedContentTypes: [.folder],
                allowsMultipleSelection: false
            ) { result in
                switch result {
                case .success(let urls):
                    guard let u = urls.first else { return }
                    vm.pickProject(u)
                case .failure(let err):
                    vm.status = "\(err)"
                }
            }
            .overlay {
                if vm.isBusy {
                    ZStack {
                        Color.black.opacity(0.08).ignoresSafeArea()
                        ProgressView()
                            .padding(18)
                            .background(.ultraThinMaterial)
                            .clipShape(RoundedRectangle(cornerRadius: 14, style: .continuous))
                    }
                }
            }
        }
    }

    private var topBar: some View {
        HStack(spacing: 10) {
            Button("Project…") { vm.showProjectPicker = true }
            Button("Build") { Task { await vm.buildDB() } }
            Button("Validate") { Task { await vm.validateDB() } }
            Button("Refresh") { Task { await vm.refreshTables() } }
            Spacer()
            Text(vm.dbRelPath)
                .font(.system(.footnote, design: .monospaced))
                .foregroundStyle(.secondary)
        }
        .disabled(vm.isBusy)
    }

    private var previewPane: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text("Preview").font(.system(.headline, design: .rounded))
                Spacer()
                Picker("Rows", selection: $vm.previewLimit) {
                    Text("50").tag(50)
                    Text("200").tag(200)
                    Text("1000").tag(1000)
                }
                .frame(width: 120)
                .onChange(of: vm.previewLimit) { _, _ in
                    vm.previewOffset = 0
                    if let t = vm.selectedTable { Task { await vm.loadPreview(t) } }
                }

                Button("◀︎") { Task { await vm.previewPrev() } }
                Button("▶︎") { Task { await vm.previewNext() } }
            }

            DataGrid(columns: vm.previewColumns, rows: vm.previewRows)
        }
    }

    private var schemaPane: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Schema").font(.system(.headline, design: .rounded))
            DataGrid(columns: vm.schemaColumns, rows: vm.schemaRows)
        }
    }

    private var sqlPane: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text("SQL").font(.system(.headline, design: .rounded))
                Spacer()
                Button("Run") { Task { await vm.runSQL() } }
                    .disabled(vm.isBusy)
            }

            TextEditor(text: $vm.query)
                .font(.system(.body, design: .monospaced))
                .frame(height: 140)
                .padding(8)
                .background(.background)
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                .overlay(
                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                        .stroke(.quaternary, lineWidth: 1)
                )

            DataGrid(columns: vm.sqlColumns, rows: vm.sqlRows)
        }
    }

    private var validatePane: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("PK checks").font(.system(.headline, design: .rounded))
            DataGrid(columns: vm.pkColumns, rows: vm.pkRows)
            Text("FK orphan checks").font(.system(.headline, design: .rounded))
            DataGrid(columns: vm.fkColumns, rows: vm.fkRows)
        }
    }

    private var bottomStatus: some View {
        HStack {
            Circle().fill(vm.isBusy ? .orange : .green).frame(width: 8, height: 8)
            Text(vm.status)
                .font(.system(.footnote, design: .rounded))
                .foregroundStyle(.secondary)
            Spacer()
        }
    }
}
