sap.ui.define([
	"sap/ui/core/mvc/ControllerExtension",
	"sap/ui/core/util/File",
	"sap/m/MessageToast",
	"sap/base/Log"
], function (ControllerExtension, File, MessageToast, Log) {
	"use strict";

	function fileSafe(value) {
		return String(value == null ? "" : value).replace(/[^A-Za-z0-9._-]+/g, "_");
	}

	/**
	 * Builds a ZIP from the message's payload and properties columns, both of
	 * which are already loaded in the binding context, so no backend call is
	 * needed. The two columns hold raw JSON strings and are written verbatim.
	 * jszip is required lazily so a load issue never blocks the extension from
	 * registering; it attaches the JSZip constructor to the global scope.
	 */
	return ControllerExtension.extend("monitoring.ext.DownloadActions", {
		onDownloadMessage: function () {
			var context = this.base.getView().getBindingContext();
			if (!context) {
				return;
			}

			var payload = context.getProperty("payload");
			var properties = context.getProperty("properties");
			var name = fileSafe(context.getProperty("serviceName") || "message")
				+ "_" + fileSafe(context.getProperty("messageId") || context.getProperty("ID"));
			var that = this;

			function fail(error) {
				Log.error("Failed to build message ZIP: " + error.message, null, "monitoring");
				var bundle = that.base.getView().getModel("i18n");
				bundle = bundle && bundle.getResourceBundle();
				MessageToast.show(bundle ? bundle.getText("downloadMessageFailed") : "Download failed.");
			}

			sap.ui.require(["sap/ui/thirdparty/jszip"], function () {
				try {
					var zip = new JSZip();
					zip.file("payload.json", payload == null ? "" : String(payload));
					zip.file("properties.json", properties == null ? "" : String(properties));
					zip.generateAsync({ type: "arraybuffer" }).then(function (buffer) {
						File.save(buffer, name, "zip", "application/zip");
					}).catch(fail);
				} catch (error) {
					fail(error);
				}
			});
		}
	});
});
