sap.ui.define([
	"sap/fe/core/AppComponent",
	"monitoring/CredentialDialog"
], function (AppComponent, CredentialDialog) {
	"use strict";

	return AppComponent.extend("monitoring.Component", {
		metadata: {
			manifest: "json"
		},

		init: function () {
			AppComponent.prototype.init.apply(this, arguments);
			// Ingestion cannot run without a browser session, so ask for one as
			// soon as the dashboard opens and whenever the session expires.
			CredentialDialog.startWatching();
		},

		exit: function () {
			CredentialDialog.stopWatching();
			if (AppComponent.prototype.exit) {
				AppComponent.prototype.exit.apply(this, arguments);
			}
		}
	});
});
