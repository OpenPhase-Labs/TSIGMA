"""
Authentication provider registry for TSIGMA.

Auth providers are self-registering plugins that handle the login flow.
Only one provider is active at a time, controlled by settings.auth_mode.
The session/authorization layer is provider-agnostic.
"""

from abc import ABC, abstractmethod
from typing import ClassVar

from fastapi import APIRouter


class BaseAuthProvider(ABC):
    """
    Base class for all authentication provider plugins.

    Subclass this and decorate with @AuthProviderRegistry.register("name")
    to create a new auth provider plugin.
    """

    name: ClassVar[str]
    description: ClassVar[str]

    @abstractmethod
    async def initialize(self) -> None:
        """
        Initialize the provider.

        Called once during app lifespan startup. Use for fetching
        OIDC discovery documents, validating config, etc.
        """
        ...

    @abstractmethod
    def get_router(self) -> APIRouter:
        """
        Return a FastAPI APIRouter with this provider's login routes.

        The router's routes will be mounted under /api/v1/auth/
        by the app lifespan.
        """
        ...


class AuthProviderRegistry:
    """
    Central registry for all auth provider plugins.

    Providers self-register using the @AuthProviderRegistry.register decorator.
    """

    _providers: dict[str, type[BaseAuthProvider]] = {}

    @classmethod
    def register(cls, name: str):
        """
        Register an auth provider plugin.

        Usage:
            @AuthProviderRegistry.register("oidc")
            class OIDCProvider(BaseAuthProvider):
                ...

        Args:
            name: Provider identifier (e.g., "local", "oidc", "oauth2").

        Returns:
            Decorator function.
        """
        def wrapper(provider_class: type[BaseAuthProvider]) -> type[BaseAuthProvider]:
            cls._providers[name] = provider_class
            return provider_class
        return wrapper

    @classmethod
    def get(cls, name: str) -> type[BaseAuthProvider]:
        """
        Get a registered provider by name.

        Args:
            name: Provider identifier.

        Returns:
            Provider class.

        Raises:
            ValueError: If provider not found.
        """
        if name not in cls._providers:
            available = ", ".join(cls._providers.keys()) or "(none)"
            raise ValueError(
                f"Unknown auth provider: {name!r}. Available: {available}"
            )
        return cls._providers[name]

    @classmethod
    def list_available(cls) -> list[str]:
        """
        List all registered provider names.

        Returns:
            List of provider name strings.
        """
        return list(cls._providers.keys())
