import SwiftUI
import UniformTypeIdentifiers

@MainActor
final class AppVM: ObservableObject {
    @Published var projectURL: URL?
    @Published var dbRelPath: String = "build/school.duckdb"
    @Published var tables: [String] = []
    @Published var selectedTable: String?
    @Published var previewColumns: [String] = []
    @Published var previewRows: [[String]] = []

    @Published var query: String = "SELECT * FROM customer LIMIT 50;"
    @Published var sqlColumns: [String] = []
    @Published var sqlRows: [[String]] = []

    @Published var status: String = "ready"
    @Published var showProjectPicker: Bool = false

    private func runner() throws -> TdbRunner {
        guard let url = projectURL else { throw TdbError(message: "Project folder not selected") }
        return TdbRunner(projectURL: url)
    }

    private func dbPathArg() throws -> [String] {
        guard let url = projectURL else { throw TdbError(message: "Project folder not selected") }
        let dbURL = url.appendingPathComponent(dbRelPath)
        return ["--db", dbURL.path]
    }

    func pickProject(_ url: URL) {
        projectURL = url
        status = "project: \(url.path)"
        Task { await refreshTables() }
    }

    func refreshTables() async {
        do {
            let r = try runner()
            let args = ["tables"] + (try dbPathArg()) + ["--json"]
            let any = try await r.runJSON(args)
            let names = (any as? [String]) ?? []
            tables = names.sorted()
            selectedTable = tables.first
            if let t = selectedTable { await loadPreview(t) }
            status = "tables: \(tables.count)"
        } catch {
            status = "\(error)"
        }
    }

    func loadPreview(_ table: String) async {
        do {
            let r = try runner()
            let args = ["head", "--table", table, "-n", "50"] + (try dbPathArg()) + ["--json"]
            let any = try await r.runJSON(args)
            let arr = (any as? [[String: Any]]) ?? []
            let cols = (arr.first.map { Array($0.keys) } ?? []).sorted()
            previewColumns = cols
            previewRows = arr.map { row in cols.map { String(describing: row[$0] ?? "") } }
            status = "preview: \(table) (\(previewRows.count) rows)"
        } catch {
            status = "\(error)"
        }
    }

    func runSQL() async {
        do {
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
            status = "\(error)"
        }
    }

    func buildDB() async {
        do {
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
            status = "\(error)"
        }
    }

    func validateDB() async {
        do {
            let r = try runner()
            guard let url = projectURL else { throw TdbError(message: "Project folder not selected") }
            let dbURL = url.appendingPathComponent(dbRelPath)
            let profile = url.appendingPathComponent(".tdb_profile.json")
            let args = ["validate", "--db", dbURL.path, "--profile", profile.path, "--json"]
            let any = try await r.runJSON(args)
            let dict = (any as? [String: Any]) ?? [:]
            let pk = (dict["pk"] as? [Any])?.count ?? 0
            let fk = (dict["fk"] as? [Any])?.count ?? 0
            status = "validate ok: pk=\(pk), fk=\(fk)"
        } catch {
            status = "\(error)"
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
                Task { await vm.loadPreview(t) }
            }
        } detail: {
            VStack(spacing: 12) {
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

                VStack(alignment: .leading, spacing: 8) {
                    Text("Preview")
                        .font(.system(.headline, design: .rounded))
                    DataGrid(columns: vm.previewColumns, rows: vm.previewRows)
                        .animation(.easeInOut(duration: 0.12), value: vm.previewRows.count)
                }

                VStack(alignment: .leading, spacing: 8) {
                    HStack {
                        Text("SQL")
                            .font(.system(.headline, design: .rounded))
                        Spacer()
                        Button("Run") { Task { await vm.runSQL() } }
                    }

                    TextEditor(text: $vm.query)
                        .font(.system(.body, design: .monospaced))
                        .frame(height: 120)
                        .padding(8)
                        .background(.background)
                        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
                        .overlay(
                            RoundedRectangle(cornerRadius: 12, style: .continuous)
                                .stroke(.quaternary, lineWidth: 1)
                        )

                    DataGrid(columns: vm.sqlColumns, rows: vm.sqlRows)
                        .animation(.easeInOut(duration: 0.12), value: vm.sqlRows.count)
                }

                HStack {
                    Circle().fill(.green).frame(width: 8, height: 8)
                    Text(vm.status)
                        .font(.system(.footnote, design: .rounded))
                        .foregroundStyle(.secondary)
                    Spacer()
                }
            }
            .padding(12)
            .navigationTitle("TDB")
        }
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
    }
}
