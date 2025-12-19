import SwiftUI

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

    @Published var tableFilter: String = ""

    @Published var previewColumns: [String] = []
    @Published var previewRows: [[String]] = []
    @Published var previewLimit: Int = 50
    @Published var previewOffset: Int = 0
    @Published var previewTotal: Int? = nil

    @Published var schemaColumns: [String] = ["column_name", "column_type", "null", "key"]
    @Published var schemaRows: [[String]] = []

    @Published var query: String = "SELECT * FROM customer LIMIT 50;"
    @Published var sqlColumns: [String] = []
    @Published var sqlRows: [[String]] = []

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
        status = "project: \(url.lastPathComponent)"
        Task { await refreshTables() }
    }

    func tryRestoreProject() {
        let path = UserDefaults.standard.string(forKey: "tdb.projectPath") ?? ""
        guard !path.isEmpty else { return }
        projectURL = URL(fileURLWithPath: path, isDirectory: true)
        status = "project: \(URL(fileURLWithPath: path).lastPathComponent)"
        Task { await refreshTables() }
    }

    var filteredTables: [String] {
        let q = tableFilter.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !q.isEmpty else { return tables }
        return tables.filter { $0.localizedCaseInsensitiveContains(q) }
    }

    func refreshTables() async {
        do {
            setBusy(true, "refresh…")
            defer { setBusy(false, status) }

            let r = try runner()
            let any = try await r.runJSON(["tables"] + (try dbPathArg()) + ["--json"])
            tables = ((any as? [String]) ?? []).sorted()

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

            _ = try await r.runJSON(["build", folder.path, "--db", dbURL.path, "--profile", profile.path, "--json"])
            status = "build ok"
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

            let any = try await r.runJSON(["validate", "--db", dbURL.path, "--profile", profile.path, "--json"])
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
            let any = try await r.runJSON(["describe", "--table", table] + (try dbPathArg()) + ["--json"])
            let rows = (any as? [[String: Any]]) ?? []
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
            previewColumns = (dict["columns"] as? [String]) ?? []
            let rowsAny = (dict["rows"] as? [[Any]]) ?? []
            previewRows = rowsAny.map { $0.map { String(describing: $0) } }

            let totalStr = previewTotal.map { " / \($0)" } ?? ""
            status = "preview \(previewOffset + 1)-\(previewOffset + previewRows.count)\(totalStr)"
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
            let any = try await r.runJSON(["sql", query] + (try dbPathArg()) + ["--json"])
            let dict = (any as? [String: Any]) ?? [:]
            sqlColumns = (dict["columns"] as? [String]) ?? []
            let rowsAny = (dict["rows"] as? [[Any]]) ?? []
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
    @AppStorage("tdb.theme") private var themeRaw: String = AppTheme.aero.rawValue

    private var theme: AppTheme { AppTheme(rawValue: themeRaw) ?? .aero }
    private var ds: DS { DS(theme: theme) }

    var body: some View {
        ZStack {
            AppBackground(ds: ds)

            NavigationSplitView {
                sidebar
            } detail: {
                detail
            }
            .navigationSplitViewStyle(.balanced)
        }
        .onAppear { vm.tryRestoreProject() }
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("Tables")
                    .font(.system(.headline, design: .rounded).weight(.semibold))
                Spacer()
            }

            TextField("Search…", text: $vm.tableFilter)
                .textFieldStyle(.roundedBorder)

            GlassCard(ds: ds) {
                ScrollView {
                    VStack(spacing: 4) {
                        ForEach(vm.filteredTables, id: \.self) { t in
                            SidebarRow(
                                ds: ds,
                                title: t,
                                icon: iconForTable(t),
                                selected: vm.selectedTable == t
                            ) {
                                vm.selectedTable = t
                                vm.previewOffset = 0
                                Task {
                                    await vm.loadPreview(t)
                                    await vm.loadSchema(t)
                                }
                            }
                        }
                    }
                    .padding(6)
                }
                .frame(minHeight: 240)
            }

            Spacer()
        }
        .padding(12)
        .frame(minWidth: 240)
    }

    private var detail: some View {
        VStack(spacing: 12) {
            topBar

            Picker("", selection: $vm.tab) {
                ForEach(MainTab.allCases) { t in
                    Text(t.rawValue).tag(t)
                }
            }
            .pickerStyle(.segmented)
            .padding(.horizontal, 2)

            Group {
                switch vm.tab {
                case .preview: previewPane
                case .schema: schemaPane
                case .sql: sqlPane
                case .validate: validatePane
                }
            }
            .animation(.snappy(duration: 0.14), value: vm.tab)

            bottomStatus
        }
        .padding(12)
        .fileImporter(isPresented: $vm.showProjectPicker,
                      allowedContentTypes: [.folder],
                      allowsMultipleSelection: false) { result in
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
                    Color.black.opacity(theme == .blade ? 0.28 : 0.12).ignoresSafeArea()
                    ProgressView()
                        .tint(ds.accent)
                        .padding(18)
                        .background(.ultraThinMaterial)
                        .overlay(SpecularOverlay())
                        .clipShape(RoundedRectangle(cornerRadius: ds.radiusXL, style: .continuous))
                        .overlay(
                            RoundedRectangle(cornerRadius: ds.radiusXL, style: .continuous)
                                .stroke(ds.cardStroke, lineWidth: ds.strokeThin)
                        )
                }
            }
        }
    }

    private var topBar: some View {
        GlassCard(ds: ds) {
            HStack(spacing: 10) {
                Button("Project…") { vm.showProjectPicker = true }
                    .buttonStyle(PillButtonStyle(ds: ds))

                Button("Build") { Task { await vm.buildDB() } }
                    .buttonStyle(PillButtonStyle(ds: ds))

                Button("Validate") { Task { await vm.validateDB() } }
                    .buttonStyle(PillButtonStyle(ds: ds))

                Button("Refresh") { Task { await vm.refreshTables() } }
                    .buttonStyle(PillButtonStyle(ds: ds))

                Spacer()

                Menu {
                    Picker("Theme", selection: $themeRaw) {
                        ForEach(AppTheme.allCases) { t in
                            Text(t.rawValue).tag(t.rawValue)
                        }
                    }
                } label: {
                    Image(systemName: "sparkles")
                        .font(.system(.body))
                        .padding(.horizontal, 10)
                        .padding(.vertical, 7)
                        .background(.thinMaterial)
                        .overlay(SpecularOverlay().opacity(0.9))
                        .clipShape(Capsule())
                        .overlay(Capsule().stroke(ds.cardStroke, lineWidth: ds.strokeThin))
                }

                Text(vm.dbRelPath)
                    .font(.system(.footnote, design: .monospaced))
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var previewPane: some View {
        GlassCard(ds: ds) {
            VStack(alignment: .leading, spacing: 10) {
                HStack {
                    Text("Preview")
                        .font(.system(.headline, design: .rounded).weight(.semibold))
                    Spacer()
                    Text("Rows").foregroundStyle(.secondary)
                    Picker("", selection: $vm.previewLimit) {
                        Text("50").tag(50)
                        Text("200").tag(200)
                        Text("1000").tag(1000)
                    }
                    .frame(width: 90)
                    .onChange(of: vm.previewLimit) { _, _ in
                        vm.previewOffset = 0
                        if let t = vm.selectedTable { Task { await vm.loadPreview(t) } }
                    }

                    Button("◀︎") { Task { await vm.previewPrev() } }.buttonStyle(PillButtonStyle(ds: ds))
                    Button("▶︎") { Task { await vm.previewNext() } }.buttonStyle(PillButtonStyle(ds: ds))
                }

                DataGrid(columns: vm.previewColumns, rows: vm.previewRows, ds: ds)
            }
        }
    }

    private var schemaPane: some View {
        GlassCard(ds: ds) {
            VStack(alignment: .leading, spacing: 10) {
                Text("Schema")
                    .font(.system(.headline, design: .rounded).weight(.semibold))
                DataGrid(columns: vm.schemaColumns, rows: vm.schemaRows, ds: ds)
            }
        }
    }

    private var sqlPane: some View {
        GlassCard(ds: ds) {
            VStack(alignment: .leading, spacing: 10) {
                HStack {
                    Text("SQL")
                        .font(.system(.headline, design: .rounded).weight(.semibold))
                    Spacer()
                    Button("Run") { Task { await vm.runSQL() } }
                        .buttonStyle(PillButtonStyle(ds: ds))
                }

                TextEditor(text: $vm.query)
                    .font(.system(.body, design: .monospaced))
                    .frame(height: 140)
                    .padding(10)
                    .background(.thinMaterial)
                    .overlay(SpecularOverlay().opacity(0.7))
                    .clipShape(RoundedRectangle(cornerRadius: ds.radiusM, style: .continuous))
                    .overlay(
                        RoundedRectangle(cornerRadius: ds.radiusM, style: .continuous)
                            .stroke(ds.cardStroke, lineWidth: ds.strokeThin)
                    )

                DataGrid(columns: vm.sqlColumns, rows: vm.sqlRows, ds: ds)
            }
        }
    }

    private var validatePane: some View {
        GlassCard(ds: ds) {
            VStack(alignment: .leading, spacing: 12) {
                Text("PK checks")
                    .font(.system(.headline, design: .rounded).weight(.semibold))
                DataGrid(columns: vm.pkColumns, rows: vm.pkRows, ds: ds)

                Text("FK orphan checks")
                    .font(.system(.headline, design: .rounded).weight(.semibold))
                DataGrid(columns: vm.fkColumns, rows: vm.fkRows, ds: ds)
            }
        }
    }

    private var bottomStatus: some View {
        HStack {
            Circle().fill(vm.isBusy ? ds.accent : .green).frame(width: 8, height: 8)
            Text(vm.status)
                .font(.system(.footnote, design: .rounded))
                .foregroundStyle(.secondary)
            Spacer()
        }
        .padding(.horizontal, 4)
    }
}
