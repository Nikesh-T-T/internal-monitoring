sap.ui.define([
	"sap/ui/core/Fragment",
	"sap/ui/model/json/JSONModel",
	"sap/ui/model/resource/ResourceModel",
	"sap/m/MessageToast",
	"sap/m/MessageBox",
	"sap/base/Log"
], function (Fragment, JSONModel, ResourceModel, MessageToast, MessageBox, Log) {
	"use strict";

	var SERVICE_URL = "/odata/v4/MonitoringService/";

	/** How often the credential state is re-checked while the app is open. */
	var WATCH_INTERVAL_MS = 15000;

	/**
	 * How long the dialog stays away after the user closed it without saving.
	 * Without this the periodic check would pop it up again seconds later.
	 */
	var SNOOZE_MS = 120000;

	var oDialog = null;
	var oDialogPromise = null;
	var iWatcher = null;
	var iDismissedAt = 0;

	var oModel = new JSONModel({
		cookie: "",
		busy: false,
		reason: "missing",
		message: ""
	});

	function readStatus() {
		return fetch(SERVICE_URL + "IngestionStatus", {
			headers: { Accept: "application/json" }
		}).then(function (oResponse) {
			if (!oResponse.ok) {
				throw new Error("HTTP " + oResponse.status);
			}
			return oResponse.json();
		}).then(function (oBody) {
			return (oBody.value && oBody.value[0]) || null;
		});
	}

	function callAction(sName, oPayload) {
		return fetch(SERVICE_URL + sName, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				Accept: "application/json"
			},
			body: JSON.stringify(oPayload)
		}).then(function (oResponse) {
			return oResponse.json().then(function (oBody) {
				if (!oResponse.ok) {
					throw new Error((oBody.error && oBody.error.message) || "HTTP " + oResponse.status);
				}
				return oBody.value;
			});
		});
	}

	var CredentialDialog = {

		/**
		 * Opens the dialog, creating it on first use.
		 *
		 * @param {string} [sReason] "missing" or "expired"; selects the explanation
		 * @returns {Promise} resolved with the dialog once it is on screen
		 */
		open: function (sReason) {
			oModel.setProperty("/reason", sReason || "missing");
			oModel.setProperty("/message", "");

			if (!oDialogPromise) {
				oDialogPromise = Fragment.load({
					name: "monitoring.view.CredentialDialog",
					controller: CredentialDialog
				}).then(function (oLoaded) {
					oDialog = oLoaded;
					oDialog.setModel(oModel, "dialog");
					// The dialog lives outside the component's view hierarchy, so it
					// needs its own resource bundle rather than inheriting one.
					oDialog.setModel(new ResourceModel({
						bundleName: "monitoring.i18n.i18n",
						supportedLocales: [""],
						fallbackLocale: ""
					}), "i18n");
					oDialog.attachAfterClose(function () {
						iDismissedAt = Date.now();
					});
					return oDialog;
				});
			}

			return oDialogPromise.then(function (oLoaded) {
				oLoaded.open();
				return oLoaded;
			});
		},

		isOpen: function () {
			return !!oDialog && oDialog.isOpen();
		},

		onCancel: function () {
			oDialog.close();
		},

		onSave: function () {
			var sCookie = (oModel.getProperty("/cookie") || "").trim();
			if (!sCookie) {
				oModel.setProperty("/message", "Paste the value of the 'Cookie' request header.");
				return;
			}
			oModel.setProperty("/busy", true);
			oModel.setProperty("/message", "");

			callAction("setCredentials", { cookie: sCookie }).then(function (sResult) {
				oModel.setProperty("/busy", false);
				oModel.setProperty("/cookie", "");
				oDialog.close();
				// A missing cookie is reported rather than rejected, so even a
				// successful call can carry a warning that is worth reading.
				if (sResult && sResult.indexOf("Warning:") !== -1) {
					MessageBox.warning(sResult);
				} else {
					MessageToast.show("Credentials stored. Polling resumes shortly.");
				}
			}).catch(function (oError) {
				oModel.setProperty("/busy", false);
				oModel.setProperty("/message", oError.message);
			});
		},

		/**
		 * Checks the credential state once and opens the dialog when the poller
		 * cannot run: either nothing was supplied yet, or the session expired.
		 */
		checkOnce: function () {
			return readStatus().then(function (oStatus) {
				if (!oStatus || CredentialDialog.isOpen()) {
					return;
				}
				var sReason = null;
				if (!oStatus.credentialsPresent) {
					sReason = "missing";
				} else if (oStatus.credentialsExpired) {
					sReason = "expired";
				}
				if (sReason && Date.now() - iDismissedAt > SNOOZE_MS) {
					CredentialDialog.open(sReason);
				}
			}).catch(function (oError) {
				// A status hiccup must never break the dashboard.
				Log.warning("Could not read the ingestion status: " + oError.message, null, "monitoring");
			});
		},

		/** Starts the periodic check; safe to call more than once. */
		startWatching: function () {
			CredentialDialog.checkOnce();
			if (iWatcher === null) {
				iWatcher = window.setInterval(CredentialDialog.checkOnce, WATCH_INTERVAL_MS);
			}
		},

		stopWatching: function () {
			if (iWatcher !== null) {
				window.clearInterval(iWatcher);
				iWatcher = null;
			}
		}
	};

	return CredentialDialog;
});
