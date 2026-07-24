import { ApplicationConfig, inject, provideAppInitializer, provideBrowserGlobalErrorListeners } from '@angular/core';

import { provideHttpClient } from '@angular/common/http';
import { AppSettingsService } from './app';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideHttpClient(),
    provideAppInitializer(() => {
      const appSettings = inject(AppSettingsService);
      return appSettings.load();
    })
  ]
};
