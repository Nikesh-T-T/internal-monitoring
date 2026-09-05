sap.ui.define([
	"sap/ui/core/util/File",
	"sap/m/MessageToast",
	"sap/base/Log",
	"sap/ui/thirdparty/jszip"
], function (File, MessageToast, Log) {
	"use strict";

	function fileSafe(value) {
		return String(value == null ? "" : value).replace(/[^A-Za-z0-9._-]+/g, "_");
	}

	function resolveContext(arg) {
		if (!arg) {
			return null;
		}
		if (typeof arg.getObject === "function" && typeof arg.getProperty === "function") {
			return arg;
		}
		if (arg.bindingContext) {
			return arg.bindingContext;
		}
		var contexts = arg.contexts || arg.selectedContexts;
		if (contexts && contexts.length) {
			return contexts[0];
		}
		if (typeof arg.getSource === "function") {
			return arg.getSource().getBindingContext();
		}
		return null;
	}

	/**
	 * Builds a ZIP from the message's payload and properties columns, both of
	 * which are already loaded in the binding context, so no backend call is
	 * needed. The two columns hold raw JSON strings and are written verbatim.
	 * jszip attaches the JSZip constructor to the global scope rather than
	 * returning it as a module value, so it is read from there.
	 */
	return {
		onDownloadMessage: function (arg) {
			var context = resolveContext(arg);
			if (!context) {
				Log.error("No binding context for message download", null, "monitoring");
				return;
			}

			var payload = context.getProperty("payload");
			var properties = context.getProperty("properties");
			var name = fileSafe(context.getProperty("serviceName") || "message")
				+ "_" + fileSafe(context.getProperty("messageId") || context.getProperty("ID"));

			function fail(error) {
				Log.error("Failed to build message ZIP: " + error.message, null, "monitoring");
				MessageToast.show("Download failed.");
			}

			try {
				var zip = new JSZip();
				zip.file("payload.json", payload == null ? "" : String(payload));
				zip.file("properties.json", properties == null ? "" : String(properties));

				// The UI5-bundled jszip is v2 (synchronous generate); v3 exposes
				// the promise-based generateAsync. Support whichever is present.
				if (typeof zip.generateAsync === "function") {
					zip.generateAsync({ type: "arraybuffer" }).then(function (buffer) {
						File.save(buffer, name, "zip", "application/zip");
					}).catch(fail);
				} else {
					var content = zip.generate({ type: "arraybuffer" });
					File.save(content, name, "zip", "application/zip");
				}
			} catch (error) {
				fail(error);
			}
		}
	};
});
