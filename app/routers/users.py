from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status

from app import crud
from app.models.user import User, UserResponse, UsersResponse
from app.services.auth import get_current_user

router = APIRouter()


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user_by_id(
    user_id: UUID | Literal["me"], current_user: User = Depends(get_current_user)
) -> Any:
    if user_id == "me":
        user_id = current_user.id
    else:
        if current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden"
            )

    user = await crud.get_user(user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    return user


@router.delete("/users/{user_id}", response_model=UserResponse)
async def delete_user_by_id(
    user_id: UUID | Literal["me"], current_user: User = Depends(get_current_user)
) -> Any:
    if user_id == "me" or current_user.role != "ADMIN":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    user = await crud.delete_user(user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )

    return user


@router.get("/users", response_model=UsersResponse)
async def get_users(
    current_user: User = Depends(get_current_user),
) -> Any:
    if current_user.role != "ADMIN":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    users = await crud.get_users()
    users_list = list(users)
    count = len(users_list)

    return {"users": users_list, "total": count}
