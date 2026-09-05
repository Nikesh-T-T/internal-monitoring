sap.ui.define([
	"sap/m/MessageToast",
	"sap/base/Log"
], function (MessageToast, Log) {
	"use strict";

	function findEditor(button) {
		var vbox = button.getParent().getParent();
		return vbox.getItems().filter(function (item) {
			return item.isA("sap.ui.codeeditor.CodeEditor");
		})[0];
	}

	function rawValue(editor) {
		var field = editor.data("field");
		var context = editor.getBindingContext();
		var value = context && field ? context.getProperty(field) : null;
		return value == null ? "" : String(value);
	}

	/**
	 * Toggles the JSON shown in a section's CodeEditor between the raw string
	 * delivered by the service and an indented copy. The editor's value is not
	 * bound to the read-only entity; it is filled from the binding context, so
	 * changing it here never writes back to the model.
	 *
	 * The toggle direction is derived from the editor's current value rather than
	 * a remembered flag: the flexible column layout reuses the same CodeEditor
	 * across rows, so a flag set while formatting one row would still be set when
	 * a different row is opened, requiring a wasted first click to clear it.
	 */
	return {
		/** Seeds the editor from its bound row. */
		format: function (value) {
			return value == null ? "" : String(value);
		},

		toggle: function (event) {
			var button = event.getSource();
			var editor = findEditor(button);
			if (!editor) {
				return;
			}

			var raw = rawValue(editor);
			var showingRaw = editor.getValue() === raw;

			if (!showingRaw) {
				editor.setValue(raw);
				button.setIcon("sap-icon://syntax");
				button.setType("Default");
				return;
			}

			try {
				editor.setValue(JSON.stringify(JSON.parse(raw), null, 2));
				button.setIcon("sap-icon://source-code");
				button.setType("Emphasized");
			} catch (error) {
				Log.warning("Content is not valid JSON: " + error.message, null, "monitoring");
				var bundle = button.getModel("i18n") && button.getModel("i18n").getResourceBundle();
				MessageToast.show(bundle ? bundle.getText("jsonInvalid") : "Content is not valid JSON.");
			}
		}
	};
});
