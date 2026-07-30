Production-ready set of custom rule files for your repository's `.roo/rules/` directory (or `.clinerules`).

Creating these as separate modular files in `.roo/rules/` ensures Roo Code and OpenCode apply the correct constraints automatically depending on whether they are generating Angular UI components, FastAPI endpoints, or database queries.

---

### File 1: `.roo/rules/01-angular-frontend.md`

```markdown
# Angular 19+ & TypeScript Development Rules

## Framework & Architecture
- **Version Standard**: Write modern Angular (v19+). Always use standalone components, directives, and pipes. Do NOT generate `NgModule` unless strictly required by a legacy library.
- **Control Flow**: Always use native control flow directives (`@if`, `@for`, `@switch`). Do NOT use `*ngIf`, `*ngFor`, or `*ngSwitch`.
- **Reactivity (Signals First)**:
  - Primary state MUST use Angular Signals (`signal()`, `computed()`, `linkedSignal()`).
  - Inputs and Outputs MUST use signal syntax: `input()`, `input.required()`, `output()`, and `model()`.
  - Do NOT use `@Input()` or `@Output()` decorators.
- **RxJS Integration**:
  - Reserve RxJS for asynchronous streams, HTTP calls, debouncing, and real-time events (WebSocket, Chart updates).
  - Convert RxJS streams to Signals using `toSignal()` at the component consumption layer.
  - Convert Signals to Observables using `toObservable()` when piping into complex RxJS operators.

## Component Structure Example
```typescript
import { Component, signal, computed, input, output } from '@angular/core';

@Component({
  selector: 'app-data-card',
  standalone: true,
  imports: [],
  template: `
    @if (isLoading()) {
      <div class="spinner">Loading...</div>
    } @else {
      <div class="card">
        <h3>{{ title() }}</h3>
        <p>Calculated Value: {{ doubleCount() }}</p>
        <button (click)="handleClick()">Update</button>
      </div>
    }
  `
})
export class DataCardComponent {
  // Inputs & Outputs
  title = input.required<string>();
  actionTriggered = output<number>();

  // Signal State
  count = signal<number>(0);
  isLoading = signal<boolean>(false);

  // Computed Properties
  doubleCount = computed(() => this.count() * 2);

  handleClick(): void {
    this.count.update(c => c + 1);
    this.actionTriggered.emit(this.count());
  }
}

```

## Styling & Component Libraries

* **Material & Kendo**: Use Angular Material or Kendo UI components for complex widgets (Data Grids, Trees, Dialogs).
* **Responsive Layout**: Rely exclusively on CSS Grid for page layouts and Flexbox for component internal alignment.
* **Theme Consistency**: Use SCSS variables/CSS custom properties for colors, padding, and elevation. Avoid inline `style=""` attributes.

```

---

### File 2: `.roo/rules/02-fastapi-backend.md`

```markdown
# Python 3.12+ & FastAPI Backend Rules

## Python 3.12+ Syntax Standards
- Use modern type hinting natively (`list[str]`, `dict[str, Any]`, `str | None`). Do NOT import `List`, `Dict`, or `Optional` from `typing`.
- Use `asyncio` for I/O bound operations. All endpoint handlers performing DB operations or HTTP requests MUST be `async def`.

## FastAPI & Pydantic V2
- **Validation Models**: Define strictly typed Pydantic models (v2 syntax using `ConfigDict`, `field_validator`).
- **Dependency Injection**: Inject database sessions, auth context, and background workers using `Depends()`.
- **API Response Contracts**: Always define `response_model` or return type annotations for OpenAPI documentation auto-generation.

## Database & Data Engineering (Oracle & Trino)
- **Parameterized SQL**: NEVER interpolate variables directly into SQL strings. Use explicit bind variables (`:param` for Oracle, `?` or `%s` for Trino) to prevent SQL injection.
- **Connection Pools**: Utilize connection pools (`oracledb.create_pool_async`) at app startup via FastAPI `lifespan` handlers. Do NOT open raw non-pooled connections per request.
- **Query Optimization**: Keep database transformation logic in the SQL layer when possible. Limit dataset fetch size using window functions or `FETCH FIRST n ROWS ONLY`.

## Backend Endpoint Example
```python
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from typing import Annotated

class MetricRequest(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    metric_id: str
    threshold: float

class MetricResponse(BaseModel):
    metric_id: str
    status: str
    computed_value: float

@router.post("/metrics/calculate", response_model=MetricResponse)
async def calculate_metric(
    payload: MetricRequest,
    db: Annotated[OracleSession, Depends(get_db_session)]
) -> MetricResponse:
    query = "SELECT value FROM system_metrics WHERE id = :metric_id"
    result = await db.fetch_one(query, {"metric_id": payload.metric_id})
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Metric not found"
        )
        
    return MetricResponse(
        metric_id=payload.metric_id,
        status="NORMAL" if result["value"] < payload.threshold else "CRITICAL",
        computed_value=result["value"]
    )

```

```

---

### File 3: `.roo/rules/03-security-and-data-contract.md`

```markdown
# Full-Stack Data Contracts & Security Rules

## TypeScript <-> Python Data Mirroring
- Maintain 1:1 structural symmetry between FastAPI Pydantic schemas and Angular TypeScript interfaces.
- Field names MUST use `camelCase` in TypeScript and `snake_case` in Python. FastApi models should enable `alias_generator` if needed.

## Security & API Design
- **Authentication**: All endpoints (except public health checks) require JWT authentication processed via standard `HTTPBearer` authorization headers.
- **Role-Based Access Control (RBAC)**: Enforce RBAC at both FastAPI dependency layer and Angular `canActivate` route guards.
- **CORS**: Explicitly configure CORS allowed origins, headers, and methods. Do NOT allow `*` wildcard in production environments.
- **Input Sanitization**: Ensure Pydantic validates inputs before database execution, and Angular auto-escapes bound template text to prevent XSS.

## Unit & Integration Testing
- **Backend**: Write test coverage using `pytest` and `httpx.AsyncClient`. Mock database pools using fixtures.
- **Frontend**: Write unit tests using `Jest` / Angular `TestBed`. Spies must mock HTTP service calls completely.

```

---

## How to Activate These Rules in VS Code

1. Create a directory named `.roo/rules/` (or `.clinerules/`) in the root of your project directory.
2. Place these three `.md` files inside it.
3. Roo Code and OpenCode will read these workspace constraints before making any code edits, ensuring all generated code automatically complies with Angular 19+, FastAPI, Python 3.12, and security standards.