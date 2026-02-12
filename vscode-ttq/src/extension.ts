import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
} from "vscode-languageclient/node";

let client: LanguageClient | undefined;

export function activate(context: vscode.ExtensionContext): void {
  // LSP server disabled — the current parser doesn't work well in an editing
  // context. Syntax highlighting and snippets still work. Uncomment the block
  // below to re-enable when a better incremental parser is available.

  /*
  const config = vscode.workspace.getConfiguration("ttq");
  const pythonPath = config.get<string>("pythonPath", "python3");

  const serverOptions: ServerOptions = {
    command: pythonPath,
    args: ["-m", "typed_tables.lsp.server"],
  };

  const outputChannel = vscode.window.createOutputChannel(
    "TTQ Language Server",
    { log: true }
  );

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "ttq" }],
    outputChannel,
  };

  client = new LanguageClient(
    "ttq-language-server",
    "TTQ Language Server",
    serverOptions,
    clientOptions
  );

  client.start();
  */
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
