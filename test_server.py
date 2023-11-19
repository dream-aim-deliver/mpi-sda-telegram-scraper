from typing import Dict, Generic, TypeVar
from fastapi import FastAPI
from pydantic import BaseModel


class ParentResponse(BaseModel):
    required_parent_field: str = "required_parent_field"


TResponseModel = TypeVar("TResponseModel", bound=ParentResponse)


class BaseRouter(Generic[TResponseModel]):
    def __init__(self, name: str, app: FastAPI):
        self.name = name
        self.app = app
        self.register_create_route()

    def main_thing(self) -> ParentResponse:
        return ParentResponse(required_parent_field="from parent")

    def register_create_route(self):
        @self.app.get(f"/{self.name}", response_model=ParentResponse)
        def parent_wrapper(parent: int):
            return self.main_thing()


class ChildResponse(ParentResponse):
    required_child_field: str = "required_child_field"


class ChildRouter(BaseRouter[ChildResponse]):
    def __init__(self, app: FastAPI):
        super().__init__("child", app)

    # def main_thing(self) -> ChildResponse:
    #     return ChildResponse(
    #         required_parent_field="required_parent_field",
    #         required_child_field="required_child_field",
    #     )

    def register_create_route(self):
        @self.app.get(f"/{self.name}", response_model=ChildResponse)
        def child_wrapper(a: int):
            parent_response: ParentResponse = super(ChildRouter, self).main_thing()
            return ChildResponse(
                required_parent_field=parent_response.required_parent_field,
                required_child_field="from child",
            )


app = FastAPI()
ChildRouter(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("test_server:app", host="localhost", port=8000, reload=True)
