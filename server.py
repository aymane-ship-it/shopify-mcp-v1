#!/usr/bin/env python3
"""
Shopify MCP Server — EXTENDED VERSION with THEMES support.
Includes: products, orders, customers, collections, inventory,
fulfillments, webhooks, THEMES, PAGES, ARTICLES, REDIRECTS, SCRIPT TAGS.
"""
import json
import os
import logging
import time
import asyncio
from typing import Optional, List, Dict, Any
import httpx
from pydantic import BaseModel, Field, ConfigDict
from mcp.server.fastmcp import FastMCP

SHOPIFY_STORE         = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN         = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_CLIENT_ID     = os.environ.get("SHOPIFY_CLIENT_ID", "")
SHOPIFY_CLIENT_SECRET = os.environ.get("SHOPIFY_CLIENT_SECRET", "")
API_VERSION           = os.environ.get("SHOPIFY_API_VERSION", "2024-10")
TOKEN_REFRESH_BUFFER  = int(os.environ.get("TOKEN_REFRESH_BUFFER", "1800"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("shopify_mcp")

PORT          = int(os.environ.get("PORT", "8000"))
MCP_TRANSPORT = os.environ.get("MCP_TRANSPORT", "streamable-http")

mcp = FastMCP("shopify_mcp", host="0.0.0.0", port=PORT, json_response=True)


class TokenManager:
    def __init__(self, store, client_id, client_secret, static_token="", refresh_buffer=1800):
        self._store = store
        self._client_id = client_id
        self._client_secret = client_secret
        self._static_token = static_token
        self._refresh_buffer = refresh_buffer
        self._access_token = ""
        self._expires_at = 0.0
        self._lock = asyncio.Lock()
        self._use_client_credentials = bool(client_id and client_secret)
        if self._use_client_credentials:
            logger.info("Token mode: client_credentials")
        elif static_token:
            self._access_token = static_token
            self._expires_at = float("inf")

    @property
    def is_expired(self):
        if not self._access_token:
            return True
        return time.time() >= (self._expires_at - self._refresh_buffer)

    async def get_token(self):
        if not self.is_expired:
            return self._access_token
        async with self._lock:
            if not self.is_expired:
                return self._access_token
            if self._use_client_credentials:
                await self._refresh_token()
            elif not self._access_token:
                raise RuntimeError("No valid token. Set SHOPIFY_ACCESS_TOKEN.")
        return self._access_token

    async def force_refresh(self):
        if not self._use_client_credentials:
            raise RuntimeError("Cannot refresh static token.")
        async with self._lock:
            await self._refresh_token()
        return self._access_token

    async def _refresh_token(self):
        url = f"https://{self._store}.myshopify.com/admin/oauth/access_token"
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=15.0,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Token refresh failed ({resp.status_code})")
            data = resp.json()
            self._access_token = data["access_token"]
            expires_in = data.get("expires_in", 86399)
            self._expires_at = time.time() + expires_in


token_manager = TokenManager(
    store=SHOPIFY_STORE,
    client_id=SHOPIFY_CLIENT_ID,
    client_secret=SHOPIFY_CLIENT_SECRET,
    static_token=SHOPIFY_TOKEN,
    refresh_buffer=TOKEN_REFRESH_BUFFER,
)


def _base_url():
    return f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{API_VERSION}"


async def _headers():
    token = await token_manager.get_token()
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}


async def _request(method, path, params=None, body=None, _retried=False):
    if not SHOPIFY_STORE:
        raise RuntimeError("Missing SHOPIFY_STORE environment variable.")
    url = f"{_base_url()}/{path}"
    headers = await _headers()
    async with httpx.AsyncClient() as client:
        resp = await client.request(method, url, headers=headers, params=params, json=body, timeout=60.0)
        if resp.status_code == 401 and not _retried and token_manager._use_client_credentials:
            await token_manager.force_refresh()
            return await _request(method, path, params=params, body=body, _retried=True)
        resp.raise_for_status()
        if resp.status_code == 204:
            return {}
        return resp.json()


def _error(e):
    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text[:500]
        return f"Shopify API error {status}: {json.dumps(detail)}"
    if isinstance(e, httpx.TimeoutException):
        return "Request timed out."
    if isinstance(e, RuntimeError):
        return str(e)
    return f"Error: {type(e).__name__}: {e}"


def _fmt(data):
    return json.dumps(data, indent=2, default=str)


# ═══════════════════════════════════════════════════════════════════════════
# PRODUCTS
# ═══════════════════════════════════════════════════════════════════════════

class ListProductsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    limit: Optional[int] = Field(default=50, ge=1, le=250)
    status: Optional[str] = None
    product_type: Optional[str] = None
    vendor: Optional[str] = None
    collection_id: Optional[int] = None
    since_id: Optional[int] = None
    fields: Optional[str] = None


@mcp.tool(name="shopify_list_products")
async def shopify_list_products(params: ListProductsInput) -> str:
    """List products."""
    try:
        p = {"limit": params.limit}
        for f in ["status", "product_type", "vendor", "collection_id", "since_id", "fields"]:
            v = getattr(params, f)
            if v is not None:
                p[f] = v
        data = await _request("GET", "products.json", params=p)
        products = data.get("products", [])
        return _fmt({"count": len(products), "products": products})
    except Exception as e:
        return _error(e)


class GetProductInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    product_id: int


@mcp.tool(name="shopify_get_product")
async def shopify_get_product(params: GetProductInput) -> str:
    """Get a product by ID."""
    try:
        data = await _request("GET", f"products/{params.product_id}.json")
        return _fmt(data.get("product", data))
    except Exception as e:
        return _error(e)


class CreateProductInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    title: str = Field(..., min_length=1)
    body_html: Optional[str] = None
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    tags: Optional[str] = None
    status: Optional[str] = "draft"
    variants: Optional[List[Dict[str, Any]]] = None
    options: Optional[List[Dict[str, Any]]] = None
    images: Optional[List[Dict[str, Any]]] = None


@mcp.tool(name="shopify_create_product")
async def shopify_create_product(params: CreateProductInput) -> str:
    """Create a new product."""
    try:
        product = {"title": params.title}
        for f in ["body_html", "vendor", "product_type", "tags", "status", "variants", "options", "images"]:
            v = getattr(params, f)
            if v is not None:
                product[f] = v
        data = await _request("POST", "products.json", body={"product": product})
        return _fmt(data.get("product", data))
    except Exception as e:
        return _error(e)


class UpdateProductInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    product_id: int
    title: Optional[str] = None
    body_html: Optional[str] = None
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    tags: Optional[str] = None
    status: Optional[str] = None
    variants: Optional[List[Dict[str, Any]]] = None


@mcp.tool(name="shopify_update_product")
async def shopify_update_product(params: UpdateProductInput) -> str:
    """Update a product."""
    try:
        product = {}
        for f in ["title", "body_html", "vendor", "product_type", "tags", "status", "variants"]:
            v = getattr(params, f)
            if v is not None:
                product[f] = v
        data = await _request("PUT", f"products/{params.product_id}.json", body={"product": product})
        return _fmt(data.get("product", data))
    except Exception as e:
        return _error(e)


class DeleteProductInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    product_id: int


@mcp.tool(name="shopify_delete_product")
async def shopify_delete_product(params: DeleteProductInput) -> str:
    """Delete a product."""
    try:
        await _request("DELETE", f"products/{params.product_id}.json")
        return f"Product {params.product_id} deleted."
    except Exception as e:
        return _error(e)


class ProductCountInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Optional[str] = None
    vendor: Optional[str] = None
    product_type: Optional[str] = None


@mcp.tool(name="shopify_count_products")
async def shopify_count_products(params: ProductCountInput) -> str:
    """Count products."""
    try:
        p = {}
        for f in ["status", "vendor", "product_type"]:
            v = getattr(params, f)
            if v is not None:
                p[f] = v
        data = await _request("GET", "products/count.json", params=p)
        return _fmt(data)
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# ORDERS
# ═══════════════════════════════════════════════════════════════════════════

class ListOrdersInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    limit: Optional[int] = Field(default=50, ge=1, le=250)
    status: Optional[str] = "any"
    financial_status: Optional[str] = None
    fulfillment_status: Optional[str] = None
    since_id: Optional[int] = None
    created_at_min: Optional[str] = None
    created_at_max: Optional[str] = None
    fields: Optional[str] = None


@mcp.tool(name="shopify_list_orders")
async def shopify_list_orders(params: ListOrdersInput) -> str:
    """List orders."""
    try:
        p = {"limit": params.limit, "status": params.status}
        for f in ["financial_status", "fulfillment_status", "since_id", "created_at_min", "created_at_max", "fields"]:
            v = getattr(params, f)
            if v is not None:
                p[f] = v
        data = await _request("GET", "orders.json", params=p)
        orders = data.get("orders", [])
        return _fmt({"count": len(orders), "orders": orders})
    except Exception as e:
        return _error(e)


class GetOrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: int


@mcp.tool(name="shopify_get_order")
async def shopify_get_order(params: GetOrderInput) -> str:
    """Get an order."""
    try:
        data = await _request("GET", f"orders/{params.order_id}.json")
        return _fmt(data.get("order", data))
    except Exception as e:
        return _error(e)


class OrderCountInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Optional[str] = "any"
    financial_status: Optional[str] = None
    fulfillment_status: Optional[str] = None


@mcp.tool(name="shopify_count_orders")
async def shopify_count_orders(params: OrderCountInput) -> str:
    """Count orders."""
    try:
        p = {"status": params.status}
        for f in ["financial_status", "fulfillment_status"]:
            v = getattr(params, f)
            if v is not None:
                p[f] = v
        data = await _request("GET", "orders/count.json", params=p)
        return _fmt(data)
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# CUSTOMERS
# ═══════════════════════════════════════════════════════════════════════════

class ListCustomersInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    limit: Optional[int] = Field(default=50, ge=1, le=250)
    since_id: Optional[int] = None
    created_at_min: Optional[str] = None
    created_at_max: Optional[str] = None
    fields: Optional[str] = None


@mcp.tool(name="shopify_list_customers")
async def shopify_list_customers(params: ListCustomersInput) -> str:
    """List customers."""
    try:
        p = {"limit": params.limit}
        for f in ["since_id", "created_at_min", "created_at_max", "fields"]:
            v = getattr(params, f)
            if v is not None:
                p[f] = v
        data = await _request("GET", "customers.json", params=p)
        customers = data.get("customers", [])
        return _fmt({"count": len(customers), "customers": customers})
    except Exception as e:
        return _error(e)


class GetCustomerInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: int


@mcp.tool(name="shopify_get_customer")
async def shopify_get_customer(params: GetCustomerInput) -> str:
    """Get a customer."""
    try:
        data = await _request("GET", f"customers/{params.customer_id}.json")
        return _fmt(data.get("customer", data))
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# COLLECTIONS
# ═══════════════════════════════════════════════════════════════════════════

class ListCollectionsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(default=50, ge=1, le=250)
    since_id: Optional[int] = None
    collection_type: Optional[str] = "custom"


@mcp.tool(name="shopify_list_collections")
async def shopify_list_collections(params: ListCollectionsInput) -> str:
    """List collections."""
    try:
        endpoint = "custom_collections.json" if params.collection_type == "custom" else "smart_collections.json"
        p = {"limit": params.limit}
        if params.since_id:
            p["since_id"] = params.since_id
        data = await _request("GET", endpoint, params=p)
        key = "custom_collections" if params.collection_type == "custom" else "smart_collections"
        collections = data.get(key, [])
        return _fmt({"count": len(collections), "collections": collections})
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# SHOP INFO
# ═══════════════════════════════════════════════════════════════════════════

class EmptyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


@mcp.tool(name="shopify_get_shop")
async def shopify_get_shop(params: EmptyInput) -> str:
    """Get shop info."""
    try:
        data = await _request("GET", "shop.json")
        return _fmt(data.get("shop", data))
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# 🎨 THEMES — NEW! For cloning Maison Aurélie homepage
# ═══════════════════════════════════════════════════════════════════════════

class ListThemesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fields: Optional[str] = None


@mcp.tool(name="shopify_list_themes")
async def shopify_list_themes(params: ListThemesInput) -> str:
    """List all themes installed on the store. The 'main' role is the active one."""
    try:
        p = {}
        if params.fields:
            p["fields"] = params.fields
        data = await _request("GET", "themes.json", params=p)
        themes = data.get("themes", [])
        return _fmt({"count": len(themes), "themes": themes})
    except Exception as e:
        return _error(e)


class GetThemeInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int


@mcp.tool(name="shopify_get_theme")
async def shopify_get_theme(params: GetThemeInput) -> str:
    """Retrieve a single theme by ID."""
    try:
        data = await _request("GET", f"themes/{params.theme_id}.json")
        return _fmt(data.get("theme", data))
    except Exception as e:
        return _error(e)


class ListAssetsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int
    fields: Optional[str] = None


@mcp.tool(name="shopify_list_theme_assets")
async def shopify_list_theme_assets(params: ListAssetsInput) -> str:
    """List all asset files in a theme."""
    try:
        p = {}
        if params.fields:
            p["fields"] = params.fields
        data = await _request("GET", f"themes/{params.theme_id}/assets.json", params=p)
        assets = data.get("assets", [])
        return _fmt({"count": len(assets), "assets": assets})
    except Exception as e:
        return _error(e)


class GetAssetInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int
    asset_key: str


@mcp.tool(name="shopify_get_theme_asset")
async def shopify_get_theme_asset(params: GetAssetInput) -> str:
    """Read content of a theme asset (CSS, Liquid, JSON, etc.)."""
    try:
        p = {"asset[key]": params.asset_key}
        data = await _request("GET", f"themes/{params.theme_id}/assets.json", params=p)
        asset = data.get("asset", data)
        return _fmt(asset)
    except Exception as e:
        return _error(e)


class UpdateAssetInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int
    asset_key: str
    value: Optional[str] = None
    src: Optional[str] = None
    source_key: Optional[str] = None


@mcp.tool(name="shopify_update_theme_asset")
async def shopify_update_theme_asset(params: UpdateAssetInput) -> str:
    """Create or update a theme asset. WARNING: overwrites existing content."""
    try:
        asset = {"key": params.asset_key}
        if params.value is not None:
            asset["value"] = params.value
        elif params.src is not None:
            asset["src"] = params.src
        elif params.source_key is not None:
            asset["source_key"] = params.source_key
        else:
            return "Error: must provide value, src, or source_key"
        data = await _request("PUT", f"themes/{params.theme_id}/assets.json", body={"asset": asset})
        return _fmt(data.get("asset", data))
    except Exception as e:
        return _error(e)


class DeleteAssetInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int
    asset_key: str


@mcp.tool(name="shopify_delete_theme_asset")
async def shopify_delete_theme_asset(params: DeleteAssetInput) -> str:
    """Delete a theme asset. Cannot be undone."""
    try:
        p = {"asset[key]": params.asset_key}
        await _request("DELETE", f"themes/{params.theme_id}/assets.json", params=p)
        return f"Asset '{params.asset_key}' deleted from theme {params.theme_id}."
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════════════

class ListPagesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(default=50, ge=1, le=250)
    since_id: Optional[int] = None


@mcp.tool(name="shopify_list_pages")
async def shopify_list_pages(params: ListPagesInput) -> str:
    """List all pages on the storefront."""
    try:
        p = {"limit": params.limit}
        if params.since_id:
            p["since_id"] = params.since_id
        data = await _request("GET", "pages.json", params=p)
        pages = data.get("pages", [])
        return _fmt({"count": len(pages), "pages": pages})
    except Exception as e:
        return _error(e)


class GetPageInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    page_id: int


@mcp.tool(name="shopify_get_page")
async def shopify_get_page(params: GetPageInput) -> str:
    """Retrieve a single page by ID."""
    try:
        data = await _request("GET", f"pages/{params.page_id}.json")
        return _fmt(data.get("page", data))
    except Exception as e:
        return _error(e)


class CreatePageInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    title: str = Field(..., min_length=1)
    body_html: Optional[str] = None
    handle: Optional[str] = None
    published: Optional[bool] = True
    template_suffix: Optional[str] = None


@mcp.tool(name="shopify_create_page")
async def shopify_create_page(params: CreatePageInput) -> str:
    """Create a new page."""
    try:
        page = {"title": params.title}
        for f in ["body_html", "handle", "published", "template_suffix"]:
            v = getattr(params, f)
            if v is not None:
                page[f] = v
        data = await _request("POST", "pages.json", body={"page": page})
        return _fmt(data.get("page", data))
    except Exception as e:
        return _error(e)


class UpdatePageInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    page_id: int
    title: Optional[str] = None
    body_html: Optional[str] = None
    handle: Optional[str] = None
    published: Optional[bool] = None
    template_suffix: Optional[str] = None


@mcp.tool(name="shopify_update_page")
async def shopify_update_page(params: UpdatePageInput) -> str:
    """Update an existing page."""
    try:
        page = {}
        for f in ["title", "body_html", "handle", "published", "template_suffix"]:
            v = getattr(params, f)
            if v is not None:
                page[f] = v
        data = await _request("PUT", f"pages/{params.page_id}.json", body={"page": page})
        return _fmt(data.get("page", data))
    except Exception as e:
        return _error(e)


class DeletePageInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    page_id: int


@mcp.tool(name="shopify_delete_page")
async def shopify_delete_page(params: DeletePageInput) -> str:
    """Delete a page."""
    try:
        await _request("DELETE", f"pages/{params.page_id}.json")
        return f"Page {params.page_id} deleted."
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# Entrypoint
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    mcp.run(transport=MCP_TRANSPORT)
