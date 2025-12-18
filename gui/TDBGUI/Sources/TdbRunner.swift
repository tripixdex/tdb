import Foundation

struct TdbError: Error, CustomStringConvertible {
    let message: String
    var description: String { message }
}

final class TdbRunner {
    let projectURL: URL

    init(projectURL: URL) {
        self.projectURL = projectURL
    }

    private var pythonURL: URL {
        projectURL.appendingPathComponent(".venv/bin/python")
    }

    func runJSON(_ args: [String]) async throws -> Any {
        let out = try await run(args)
        return try JSONSerialization.jsonObject(with: out, options: [])
    }

    func run(_ args: [String]) async throws -> Data {
        let p = pythonURL.path
        let fm = FileManager.default
        guard fm.isExecutableFile(atPath: p) else {
            throw TdbError(message: "Python not found: \(p)")
        }

        let proc = Process()
        proc.executableURL = pythonURL
        proc.currentDirectoryURL = projectURL
        proc.arguments = ["-m", "tdb"] + args

        let outPipe = Pipe()
        let errPipe = Pipe()
        proc.standardOutput = outPipe
        proc.standardError = errPipe

        try proc.run()
        proc.waitUntilExit()

        let outData = outPipe.fileHandleForReading.readDataToEndOfFile()
        let errData = errPipe.fileHandleForReading.readDataToEndOfFile()
        let errStr = String(data: errData, encoding: .utf8) ?? ""

        guard proc.terminationStatus == 0 else {
            throw TdbError(message: errStr.isEmpty ? "tdb failed" : errStr)
        }

        return outData
    }
}
