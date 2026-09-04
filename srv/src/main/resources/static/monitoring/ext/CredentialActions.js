sap.ui.define([
	"monitoring/CredentialDialog"
], function (CredentialDialog) {
	"use strict";

	/**
	 * Custom actions for the Fiori elements pages. The dialog opens by itself
	 * when credentials are missing or expired; this makes it reachable at any
	 * time, for instance to replace a session before it runs out.
	 */
	return {
		openCredentialDialog: function () {
			CredentialDialog.open("manual");
		}
	};
});
